// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/aggregator.h"

#include <math.h>
#include <algorithm>
#include <set>
#include <sstream>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/slot-ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

namespace impala {

const char* Aggregator::LLVM_CLASS_NAME = "class.impala::Aggregator";

Aggregator::Aggregator(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_id_(tnode.agg_node.intermediate_tuple_id),
    intermediate_tuple_desc_(descs.GetTupleDescriptor(intermediate_tuple_id_)),
    output_tuple_id_(tnode.agg_node.output_tuple_id),
    output_tuple_desc_(descs.GetTupleDescriptor(output_tuple_id_)),
    needs_finalize_(tnode.agg_node.need_finalize),
    build_timer_(NULL) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
  if (is_streaming_preagg_) {
    DCHECK(conjunct_evals_.empty()) << "Preaggs have no conjuncts";
    DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
    DCHECK(limit_ == -1) << "Preaggs have no limits";
  }
}

Status Aggregator::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));

  DCHECK(intermediate_tuple_desc_ != nullptr);
  DCHECK(output_tuple_desc_ != nullptr);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(), output_tuple_desc_->slots().size());
  const RowDescriptor& row_desc = *child(0)->row_desc();
  RETURN_IF_ERROR(ScalarExpr::Create(
      tnode.agg_node.grouping_exprs, row_desc, state, &grouping_exprs_));

  // Construct build exprs from intermediate_row_desc_
  for (int i = 0; i < grouping_exprs_.size(); ++i) {
    SlotDescriptor* desc = intermediate_tuple_desc_->slots()[i];
    DCHECK(desc->type().type == TYPE_NULL || desc->type() == grouping_exprs_[i]->type());
    // Hack to avoid TYPE_NULL SlotRefs.
    SlotRef* build_expr =
        pool_->Add(desc->type().type != TYPE_NULL ? new SlotRef(desc) :
                                                    new SlotRef(desc, TYPE_BOOLEAN));
    build_exprs_.push_back(build_expr);
    RETURN_IF_ERROR(build_expr->Init(intermediate_row_desc_, state));
    if (build_expr->type().IsVarLenStringType()) string_grouping_exprs_.push_back(i);
  }

  int j = grouping_exprs_.size();
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i, ++j) {
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    AggFn* agg_fn;
    RETURN_IF_ERROR(AggFn::Create(tnode.agg_node.aggregate_functions[i], row_desc,
        *intermediate_slot_desc, *output_slot_desc, state, &agg_fn));
    agg_fns_.push_back(agg_fn);
    needs_serialize_ |= agg_fn->SupportsSerialize();
  }
  return Status::OK();
}

Status Aggregator::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(ExecNode::Prepare(state));
  state_ = state;

  singleton_tuple_pool_.reset(new MemPool(mem_tracker()));

  ht_resize_timer_ = ADD_TIMER(runtime_profile(), "HTResizeTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  num_hash_buckets_ = ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
  partitions_created_ = ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
  largest_partition_percent_ =
      runtime_profile()->AddHighWaterMarkCounter("LargestPartitionPercent", TUnit::UNIT);
  if (is_streaming_preagg_) {
    runtime_profile()->AppendExecOption("Streaming Preaggregation");
    streaming_timer_ = ADD_TIMER(runtime_profile(), "StreamingTime");
    num_passthrough_rows_ =
        ADD_COUNTER(runtime_profile(), "RowsPassedThrough", TUnit::UNIT);
    preagg_estimated_reduction_ =
        ADD_COUNTER(runtime_profile(), "ReductionFactorEstimate", TUnit::DOUBLE_VALUE);
    preagg_streaming_ht_min_reduction_ = ADD_COUNTER(
        runtime_profile(), "ReductionFactorThresholdToExpand", TUnit::DOUBLE_VALUE);
  } else {
    build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
    num_row_repartitioned_ =
        ADD_COUNTER(runtime_profile(), "RowsRepartitioned", TUnit::UNIT);
    num_repartitions_ = ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
    num_spilled_partitions_ =
        ADD_COUNTER(runtime_profile(), "SpilledPartitions", TUnit::UNIT);
    max_partition_level_ =
        runtime_profile()->AddHighWaterMarkCounter("MaxPartitionLevel", TUnit::UNIT);
  }

  RETURN_IF_ERROR(AggFnEvaluator::Create(
      agg_fns_, state, pool_, expr_perm_pool(), expr_results_pool(), &agg_fn_evals_));

  if (!grouping_exprs_.empty()) {
    RETURN_IF_ERROR(HashTableCtx::Create(pool_, state, build_exprs_, grouping_exprs_,
        true, vector<bool>(build_exprs_.size(), true), state->fragment_hash_seed(),
        MAX_PARTITION_DEPTH, 1, expr_perm_pool(), expr_results_pool(),
        expr_results_pool(), &ht_ctx_));
  }
  AddCodegenDisabledMessage(state);
  return Status::OK();
}

void Aggregator::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  TPrefetchMode::type prefetch_mode = state_->query_options().prefetch_mode;
  Status codegen_status = is_streaming_preagg_ ?
      CodegenProcessBatchStreaming(codegen, prefetch_mode) :
      CodegenProcessBatch(codegen, prefetch_mode);
  runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
}

Status Aggregator::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));

  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!buffer_pool_client()->is_registered() && !grouping_exprs_.empty()) {
    DCHECK_GE(resource_profile_.min_reservation, MinReservation());
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  if (ht_ctx_.get() != nullptr) RETURN_IF_ERROR(ht_ctx_->Open(state));
  RETURN_IF_ERROR(AggFnEvaluator::Open(agg_fn_evals_, state));
  if (grouping_exprs_.empty()) {
    // Create the single output tuple for this non-grouping agg. This must happen after
    // opening the aggregate evaluators.
    singleton_output_tuple_ =
        ConstructSingletonOutputTuple(agg_fn_evals_, singleton_tuple_pool_.get());
    // Check for failures during AggFnEvaluator::Init().
    RETURN_IF_ERROR(state_->GetQueryStatus());
    singleton_output_tuple_returned_ = false;
  } else {
    if (ht_allocator_ == nullptr) {
      // Allocate 'serialize_stream_' and 'ht_allocator_' on the first Open() call.
      ht_allocator_.reset(new Suballocator(state_->exec_env()->buffer_pool(),
          buffer_pool_client(), resource_profile_.spillable_buffer_size));

      if (!is_streaming_preagg_ && needs_serialize_) {
        serialize_stream_.reset(new BufferedTupleStream(state, &intermediate_row_desc_,
            buffer_pool_client(), resource_profile_.spillable_buffer_size,
            resource_profile_.max_row_buffer_size));
        RETURN_IF_ERROR(serialize_stream_->Init(id(), false));
        bool got_buffer;
        // Reserve the memory for 'serialize_stream_' so we don't need to scrounge up
        // another buffer during spilling.
        RETURN_IF_ERROR(serialize_stream_->PrepareForWrite(&got_buffer));
        DCHECK(got_buffer) << "Accounted in min reservation"
                           << buffer_pool_client()->DebugString();
        DCHECK(serialize_stream_->has_write_iterator());
      }
    }
    RETURN_IF_ERROR(CreateHashPartitions(0));
  }

  // Streaming preaggregations do all processing in GetNext().
  if (is_streaming_preagg_) return Status::OK();

  RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  // Read all the rows from the child and process them.
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));

    if (UNLIKELY(VLOG_ROW_IS_ON)) {
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        VLOG_ROW << "input row: " << PrintRow(row, *children_[0]->row_desc());
      }
    }

    TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
    SCOPED_TIMER(build_timer_);
    if (grouping_exprs_.empty()) {
      if (process_batch_no_grouping_fn_ != NULL) {
        RETURN_IF_ERROR(process_batch_no_grouping_fn_(this, &batch));
      } else {
        RETURN_IF_ERROR(ProcessBatchNoGrouping(&batch));
      }
    } else {
      // There is grouping, so we will do partitioned aggregation.
      if (process_batch_fn_ != NULL) {
        RETURN_IF_ERROR(process_batch_fn_(this, &batch, prefetch_mode, ht_ctx_.get()));
      } else {
        RETURN_IF_ERROR(ProcessBatch<false>(&batch, prefetch_mode, ht_ctx_.get()));
      }
    }
    batch.Reset();
  } while (!eos);

  // The child can be closed at this point in most cases because we have consumed all of
  // the input from the child and transfered ownership of the resources we need. The
  // exception is if we are inside a subplan expecting to call Open()/GetNext() on the
  // child again,
  if (!IsInSubplan()) child(0)->Close(state);
  child_eos_ = true;

  // Done consuming child(0)'s input. Move all the partitions in hash_partitions_
  // to spilled_partitions_ or aggregated_partitions_. We'll finish the processing in
  // GetNext().
  if (!grouping_exprs_.empty()) {
    RETURN_IF_ERROR(MoveHashPartitions(child(0)->rows_returned()));
  }
  return Status::OK();
}

Status Aggregator::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  if (grouping_exprs_.empty()) {
    // There was no grouping, so evaluate the conjuncts and return the single result row.
    // We allow calling GetNext() after eos, so don't return this row again.
    if (!singleton_output_tuple_returned_) GetSingletonOutput(row_batch);
    singleton_output_tuple_returned_ = true;
    *eos = true;
    return Status::OK();
  }

  if (!child_eos_) {
    // For streaming preaggregations, we process rows from the child as we go.
    DCHECK(is_streaming_preagg_);
    RETURN_IF_ERROR(GetRowsStreaming(state, row_batch));
  } else if (!partition_eos_) {
    RETURN_IF_ERROR(GetRowsFromPartition(state, row_batch));
  }

  *eos = partition_eos_ && child_eos_;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status Aggregator::Reset(RuntimeState* state) {
  DCHECK(!is_streaming_preagg_) << "Cannot reset preaggregation";
  if (!grouping_exprs_.empty()) {
    child_eos_ = false;
    partition_eos_ = false;
    // Reset the HT and the partitions for this grouping agg.
    ht_ctx_->set_level(0);
    ClosePartitions();
  }
  return ExecNode::Reset(state);
}

void Aggregator::Close(RuntimeState* state) {
  if (is_closed()) return;

  if (!singleton_output_tuple_returned_) {
    GetOutputTuple(agg_fn_evals_, singleton_output_tuple_, singleton_tuple_pool_.get());
  }

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs
  if (output_partition_ != NULL) {
    CleanupHashTbl(output_partition_->agg_fn_evals, output_iterator_);
    output_partition_->Close(false);
  }

  ClosePartitions();

  child_batch_.reset();

  // Close all the agg-fn-evaluators
  AggFnEvaluator::Close(agg_fn_evals_, state);

  if (singleton_tuple_pool_.get() != nullptr) singleton_tuple_pool_->FreeAll();
  if (ht_ctx_.get() != nullptr) ht_ctx_->Close(state);
  ht_ctx_.reset();
  if (serialize_stream_.get() != nullptr) {
    serialize_stream_->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  ScalarExpr::Close(grouping_exprs_);
  ScalarExpr::Close(build_exprs_);
  AggFn::Close(agg_fns_);
  ExecNode::Close(state);
}

// TODO: codegen this function.
void Aggregator::InitAggSlots(
    const vector<AggFnEvaluator*>& agg_fn_evals, Tuple* intermediate_tuple) {
  vector<SlotDescriptor*>::const_iterator slot_desc =
      intermediate_tuple_desc_->slots().begin() + grouping_exprs_.size();
  for (int i = 0; i < agg_fn_evals.size(); ++i, ++slot_desc) {
    // To minimize branching on the UpdateTuple path, initialize the result value so that
    // the Add() UDA function can ignore the NULL bit of its destination value. E.g. for
    // SUM(), if we initialize the destination value to 0 (with the NULL bit set), we can
    // just start adding to the destination value (rather than repeatedly checking the
    // destination NULL bit. The codegen'd version of UpdateSlot() exploits this to
    // eliminate a branch per value.
    //
    // For boolean and numeric types, the default values are false/0, so the nullable
    // aggregate functions SUM() and AVG() produce the correct result. For MIN()/MAX(),
    // initialize the value to max/min possible value for the same effect.
    AggFnEvaluator* eval = agg_fn_evals[i];
    eval->Init(intermediate_tuple);

    DCHECK(agg_fns_[i] == &(eval->agg_fn()));
    const AggFn* agg_fn = agg_fns_[i];
    const AggFn::AggregationOp agg_op = agg_fn->agg_op();
    if ((agg_op == AggFn::MIN || agg_op == AggFn::MAX)
        && !agg_fn->intermediate_type().IsStringType()
        && !agg_fn->intermediate_type().IsTimestampType()) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      if (agg_op == AggFn::MIN) {
        default_value_ptr = default_value.SetToMax((*slot_desc)->type());
      } else {
        DCHECK_EQ(agg_op, AggFn::MAX);
        default_value_ptr = default_value.SetToMin((*slot_desc)->type());
      }
      RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
    }
  }
}

void Aggregator::UpdateTuple(
    AggFnEvaluator** agg_fn_evals, Tuple* tuple, TupleRow* row, bool is_merge) noexcept {
  DCHECK(tuple != NULL || agg_fns_.empty());
  for (int i = 0; i < agg_fns_.size(); ++i) {
    if (is_merge) {
      agg_fn_evals[i]->Merge(row->GetTuple(0), tuple);
    } else {
      agg_fn_evals[i]->Add(row, tuple);
    }
  }
}

Tuple* Aggregator::GetOutputTuple(
    const vector<AggFnEvaluator*>& agg_fn_evals, Tuple* tuple, MemPool* pool) {
  DCHECK(tuple != NULL || agg_fn_evals.empty()) << tuple;
  Tuple* dst = tuple;
  if (needs_finalize_ && intermediate_tuple_id_ != output_tuple_id_) {
    dst = Tuple::Create(output_tuple_desc_->byte_size(), pool);
  }
  if (needs_finalize_) {
    AggFnEvaluator::Finalize(agg_fn_evals, tuple, dst);
  } else {
    AggFnEvaluator::Serialize(agg_fn_evals, tuple);
  }
  // Copy grouping values from tuple to dst.
  // TODO: Codegen this.
  if (dst != tuple) {
    int num_grouping_slots = grouping_exprs_.size();
    for (int i = 0; i < num_grouping_slots; ++i) {
      SlotDescriptor* src_slot_desc = intermediate_tuple_desc_->slots()[i];
      SlotDescriptor* dst_slot_desc = output_tuple_desc_->slots()[i];
      bool src_slot_null = tuple->IsNull(src_slot_desc->null_indicator_offset());
      void* src_slot = NULL;
      if (!src_slot_null) src_slot = tuple->GetSlot(src_slot_desc->tuple_offset());
      RawValue::Write(src_slot, dst, dst_slot_desc, NULL);
    }
  }
  return dst;
}

string Aggregator::DebugString(int indentation_level) const {
  stringstream ss;
  DebugString(indentation_level, &ss);
  return ss.str();
}

void Aggregator::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "Aggregator("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_ << " needs_finalize=" << needs_finalize_
       << " grouping_exprs=" << ScalarExpr::DebugString(grouping_exprs_)
       << " agg_exprs=" << AggFn::DebugString(agg_fns_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(AggFnEvaluator* agg_expr_eval, AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col), which is constructed directly with the IRBuilder, is:
//
// define void @UpdateSlot(%"class.impala::AggFnEvaluator"* %agg_fn_eval,
//     <{ double, i8 }>* %agg_tuple, %"class.impala::TupleRow"* %row) #33 {
// entry:
//   %input_evals_vector = call %"class.impala::ScalarExprEvaluator"**
//       @_ZNK6impala14AggFnEvaluator11input_evalsEv(
//           %"class.impala::AggFnEvaluator"* %agg_fn_eval)
//   %0 = getelementptr %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %input_evals_vector, i32 0
//   %input_eval = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %0
//   %input0 = call { i8, double } @GetSlotRef(%"class.impala::ScalarExprEvaluator"*
//       %input_eval, %"class.impala::TupleRow"* %row)
//   %dst_slot_ptr = getelementptr inbounds <{ double, i8 }>,
//       <{ double, i8 }>* %agg_tuple, i32 0, i32 0
//   %dst_val = load double, double* %dst_slot_ptr
//   %1 = extractvalue { i8, double } %input0, 0
//   %is_null = trunc i8 %1 to i1
//   br i1 %is_null, label %ret, label %not_null
//
// ret:                                              ; preds = %not_null, %entry
//   ret void
//
// not_null:                                         ; preds = %entry
//   %val = extractvalue { i8, double } %input0, 1
//   %2 = fadd double %dst_val, %val
//   %3 = bitcast <{ double, i8 }>* %agg_tuple to i8*
//   %null_byte_ptr = getelementptr inbounds i8, i8* %3, i32 8
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_cleared = and i8 %null_byte, -2
//   store i8 %null_bit_cleared, i8* %null_byte_ptr
//   store double %2, double* %dst_slot_ptr
//   br label %ret
// }
//
// The IR for ndv(timestamp_col), which uses the UDA interface, is:
//
// define void @UpdateSlot(%"class.impala::AggFnEvaluator"* %agg_fn_eval,
//     <{ [1024 x i8] }>* %agg_tuple,
//     %"class.impala::TupleRow"* %row) #39 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i8* }
//   %0 = alloca { i64, i64 }
//   %input_evals_vector = call %"class.impala::ScalarExprEvaluator"**
//       @_ZNK6impala14AggFnEvaluator11input_evalsEv(
//           %"class.impala::AggFnEvaluator"* %agg_fn_eval)
//   %1 = getelementptr %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %input_evals_vector, i32 0
//   %input_eval = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %1
//   %input0 = call { i64, i64 } @GetSlotRef(
//       %"class.impala::ScalarExprEvaluator"* %input_eval,
//       %"class.impala::TupleRow"* %row)
//   %dst_slot_ptr = getelementptr inbounds <{ [1024 x i8] }>,
//       <{ [1024 x i8] }>* %agg_tuple, i32 0, i32 0
//   %2 = bitcast [1024 x i8]* %dst_slot_ptr to i8*
//   %dst = insertvalue { i64, i8* } zeroinitializer, i8* %2, 1
//   %3 = extractvalue { i64, i8* } %dst, 0
//   %4 = and i64 %3, 4294967295
//   %5 = or i64 %4, 4398046511104
//   %dst1 = insertvalue { i64, i8* } %dst, i64 %5, 0
//   %agg_fn_ctx = call %"class.impala_udf::FunctionContext"*
//       @_ZNK6impala14AggFnEvaluator10agg_fn_ctxEv(
//          %"class.impala::AggFnEvaluator"* %agg_fn_eval)
//   store { i64, i64 } %input0, { i64, i64 }* %0
//   %input_unlowered_ptr =
//       bitcast { i64, i64 }* %0 to %"struct.impala_udf::TimestampVal"*
//   store { i64, i8* } %dst1, { i64, i8* }* %dst_lowered_ptr
//   %dst_unlowered_ptr =
//       bitcast { i64, i8* }* %dst_lowered_ptr to %"struct.impala_udf::StringVal"*
//   call void @"void impala::AggregateFunctions::HllUpdate<impala_udf::TimestampVal>"(
//       %"class.impala_udf::FunctionContext"* %agg_fn_ctx,
//       %"struct.impala_udf::TimestampVal"* %input_unlowered_ptr,
//       %"struct.impala_udf::StringVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i8* }, { i64, i8* }* %dst_lowered_ptr
//   br label %ret
//
// ret:                                              ; preds = %entry
//   ret void
// }
//
Status Aggregator::CodegenUpdateSlot(LlvmCodeGen* codegen, int agg_fn_idx,
    SlotDescriptor* slot_desc, llvm::Function** fn) {
  llvm::PointerType* agg_fn_eval_type = codegen->GetStructPtrType<AggFnEvaluator>();
  llvm::StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  if (tuple_struct == NULL) {
    return Status("Aggregator::CodegenUpdateSlot(): failed to generate "
                  "intermediate tuple desc");
  }
  llvm::PointerType* tuple_ptr_type = codegen->GetPtrType(tuple_struct);
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();

  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_fn_eval", agg_fn_eval_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmBuilder builder(codegen->context());
  llvm::Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, &args[0]);
  llvm::Value* agg_fn_eval_arg = args[0];
  llvm::Value* agg_tuple_arg = args[1];
  llvm::Value* row_arg = args[2];

  // Get the vector of input expressions' evaluators.
  llvm::Value* input_evals_vector = codegen->CodegenCallFunction(&builder,
      IRFunction::AGG_FN_EVALUATOR_INPUT_EVALUATORS, agg_fn_eval_arg,
      "input_evals_vector");

  AggFn* agg_fn = agg_fns_[agg_fn_idx];
  const int num_inputs = agg_fn->GetNumChildren();
  DCHECK_GE(num_inputs, 1);
  vector<CodegenAnyVal> input_vals;
  for (int i = 0; i < num_inputs; ++i) {
    ScalarExpr* input_expr = agg_fn->GetChild(i);
    llvm::Function* input_expr_fn;
    RETURN_IF_ERROR(input_expr->GetCodegendComputeFn(codegen, &input_expr_fn));
    DCHECK(input_expr_fn != NULL);

    // Call input expr function with the matching evaluator to get src slot value.
    llvm::Value* input_eval =
        codegen->CodegenArrayAt(&builder, input_evals_vector, i, "input_eval");
    string input_name = Substitute("input$0", i);
    CodegenAnyVal input_val = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        input_expr->type(), input_expr_fn,
        llvm::ArrayRef<llvm::Value*>({input_eval, row_arg}), input_name.c_str());
    input_vals.push_back(input_val);
  }

  AggFn::AggregationOp agg_op = agg_fn->agg_op();
  const ColumnType& dst_type = agg_fn->intermediate_type();
  bool dst_is_int_or_float_or_bool = dst_type.IsIntegerType()
      || dst_type.IsFloatingPointType() || dst_type.IsBooleanType();
  bool dst_is_numeric_or_bool = dst_is_int_or_float_or_bool || dst_type.IsDecimalType();

  llvm::BasicBlock* ret_block = llvm::BasicBlock::Create(codegen->context(), "ret", *fn);

  // Emit the code to compute 'result' and set the NULL indicator if needed. First check
  // for special cases where we can emit a very simple instruction sequence, then fall
  // back to the general-purpose approach of calling the cross-compiled builtin UDA.
  CodegenAnyVal& src = input_vals[0];

  // 'dst_slot_ptr' points to the slot in the aggregate tuple to update.
  llvm::Value* dst_slot_ptr = builder.CreateStructGEP(
      NULL, agg_tuple_arg, slot_desc->llvm_field_idx(), "dst_slot_ptr");
  // TODO: consider moving the following codegen logic to AggFn.
  if (agg_op == AggFn::COUNT) {
    src.CodegenBranchIfNull(&builder, ret_block);
    llvm::Value* dst_value = builder.CreateLoad(dst_slot_ptr, "dst_val");
    llvm::Value* result = agg_fn->is_merge() ?
        builder.CreateAdd(dst_value, src.GetVal(), "count_sum") :
        builder.CreateAdd(dst_value, codegen->GetI64Constant(1), "count_inc");
    builder.CreateStore(result, dst_slot_ptr);
    DCHECK(!slot_desc->is_nullable());
  } else if ((agg_op == AggFn::MIN || agg_op == AggFn::MAX) && dst_is_numeric_or_bool) {
    bool is_min = agg_op == AggFn::MIN;
    src.CodegenBranchIfNull(&builder, ret_block);
    codegen->CodegenMinMax(
        &builder, slot_desc->type(), src.GetVal(), dst_slot_ptr, is_min, *fn);

    // Dst may have been NULL, make sure to unset the NULL bit.
    DCHECK(slot_desc->is_nullable());
    slot_desc->CodegenSetNullIndicator(
        codegen, &builder, agg_tuple_arg, codegen->false_value());
  } else if (agg_op == AggFn::SUM && dst_is_int_or_float_or_bool) {
    src.CodegenBranchIfNull(&builder, ret_block);
    llvm::Value* dst_value = builder.CreateLoad(dst_slot_ptr, "dst_val");
    llvm::Value* result = dst_type.IsFloatingPointType() ?
        builder.CreateFAdd(dst_value, src.GetVal()) :
        builder.CreateAdd(dst_value, src.GetVal());
    builder.CreateStore(result, dst_slot_ptr);

    if (slot_desc->is_nullable()) {
      slot_desc->CodegenSetNullIndicator(
          codegen, &builder, agg_tuple_arg, codegen->false_value());
    } else {
      // 'slot_desc' is not nullable if the aggregate function is sum_init_zero(),
      // because the slot is initialized to be zero and the null bit is nonexistent.
      DCHECK_EQ(agg_fn->fn_name(), "sum_init_zero");
    }
  } else {
    // The remaining cases are implemented using the UDA interface.
    // Create intermediate argument 'dst' from 'dst_value'
    CodegenAnyVal dst = CodegenAnyVal::GetNonNullVal(codegen, &builder, dst_type, "dst");

    // For a subset of builtins we generate a different code sequence that exploits two
    // properties of the builtins. First, NULL input values can be skipped. Second, the
    // value of the slot was initialized in the right way in InitAggSlots() (e.g. 0 for
    // SUM) that we get the right result if UpdateSlot() pretends that the NULL bit of
    // 'dst' is unset. Empirically this optimisation makes TPC-H Q1 5-10% faster.
    bool special_null_handling = !agg_fn->intermediate_type().IsStringType()
        && !agg_fn->intermediate_type().IsTimestampType()
        && (agg_op == AggFn::MIN || agg_op == AggFn::MAX || agg_op == AggFn::SUM
               || agg_op == AggFn::AVG || agg_op == AggFn::NDV);
    if (slot_desc->is_nullable()) {
      if (special_null_handling) {
        src.CodegenBranchIfNull(&builder, ret_block);
        slot_desc->CodegenSetNullIndicator(
            codegen, &builder, agg_tuple_arg, codegen->false_value());
      } else {
        dst.SetIsNull(slot_desc->CodegenIsNull(codegen, &builder, agg_tuple_arg));
      }
    }
    dst.LoadFromNativePtr(dst_slot_ptr);

    // Get the FunctionContext object for the AggFnEvaluator.
    llvm::Function* get_agg_fn_ctx_fn =
        codegen->GetFunction(IRFunction::AGG_FN_EVALUATOR_AGG_FN_CTX, false);
    DCHECK(get_agg_fn_ctx_fn != NULL);
    llvm::Value* agg_fn_ctx_val =
        builder.CreateCall(get_agg_fn_ctx_fn, {agg_fn_eval_arg}, "agg_fn_ctx");

    // Call the UDA to update/merge 'src' into 'dst', with the result stored in
    // 'updated_dst_val'.
    CodegenAnyVal updated_dst_val;
    RETURN_IF_ERROR(CodegenCallUda(
        codegen, &builder, agg_fn, agg_fn_ctx_val, input_vals, dst, &updated_dst_val));
    // Copy the value back to the slot. In the FIXED_UDA_INTERMEDIATE case, the
    // UDA function writes directly to the slot so there is nothing to copy.
    if (dst_type.type != TYPE_FIXED_UDA_INTERMEDIATE) {
      updated_dst_val.StoreToNativePtr(dst_slot_ptr);
    }

    if (slot_desc->is_nullable() && !special_null_handling) {
      // Set NULL bit in the slot based on the return value.
      llvm::Value* result_is_null = updated_dst_val.GetIsNull("result_is_null");
      slot_desc->CodegenSetNullIndicator(
          codegen, &builder, agg_tuple_arg, result_is_null);
    }
  }
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  // Avoid producing huge UpdateTuple() function after inlining - LLVM's optimiser
  // memory/CPU usage scales super-linearly with function size.
  // E.g. compute stats on all columns of a 1000-column table previously took 4 minutes to
  // codegen because all the UpdateSlot() functions were inlined.
  if (agg_fn_idx >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("Aggregator::CodegenUpdateSlot(): codegen'd "
                  "UpdateSlot() function failed verification, see log");
  }
  return Status::OK();
}

Status Aggregator::CodegenCallUda(LlvmCodeGen* codegen, LlvmBuilder* builder,
    AggFn* agg_fn, llvm::Value* agg_fn_ctx_val, const vector<CodegenAnyVal>& input_vals,
    const CodegenAnyVal& dst_val, CodegenAnyVal* updated_dst_val) {
  llvm::Function* uda_fn;
  RETURN_IF_ERROR(agg_fn->CodegenUpdateOrMergeFunction(codegen, &uda_fn));

  // Set up arguments for call to UDA, which are the FunctionContext*, followed by
  // pointers to all input values, followed by a pointer to the destination value.
  vector<llvm::Value*> uda_fn_args;
  uda_fn_args.push_back(agg_fn_ctx_val);

  // Create pointers to input args to pass to uda_fn. We must use the unlowered type,
  // e.g. IntVal, because the UDA interface expects the values to be passed as const
  // references to the classes.
  DCHECK_EQ(agg_fn->GetNumChildren(), input_vals.size());
  for (int i = 0; i < input_vals.size(); ++i) {
    uda_fn_args.push_back(input_vals[i].GetUnloweredPtr("input_unlowered_ptr"));
  }

  // Create pointer to dst to pass to uda_fn. We must use the unlowered type for the
  // same reason as above.
  llvm::Value* dst_lowered_ptr = dst_val.GetLoweredPtr("dst_lowered_ptr");
  const ColumnType& dst_type = agg_fn->intermediate_type();
  llvm::Type* dst_unlowered_ptr_type =
      CodegenAnyVal::GetUnloweredPtrType(codegen, dst_type);
  llvm::Value* dst_unlowered_ptr = builder->CreateBitCast(
      dst_lowered_ptr, dst_unlowered_ptr_type, "dst_unlowered_ptr");
  uda_fn_args.push_back(dst_unlowered_ptr);

  // Call 'uda_fn'
  builder->CreateCall(uda_fn, uda_fn_args);

  // Convert intermediate 'dst_arg' back to the native type.
  llvm::Value* anyval_result = builder->CreateLoad(dst_lowered_ptr, "anyval_result");

  *updated_dst_val = CodegenAnyVal(codegen, builder, dst_type, anyval_result);
  return Status::OK();
}

// IR codegen for the UpdateTuple loop.  This loop is query specific and based on the
// aggregate functions.  The function signature must match the non- codegen'd UpdateTuple
// exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @UpdateTuple(%"class.impala::Aggregator"* %this_ptr,
//     %"class.impala::AggFnEvaluator"** %agg_fn_evals, %"class.impala::Tuple"* %tuple,
//     %"class.impala::TupleRow"* %row, i1 %is_merge) #33 {
// entry:
//   %tuple1 = bitcast %"class.impala::Tuple"* %tuple to <{ i64, i64, double, i8 }>*
//   %src_slot = getelementptr inbounds <{ i64, i64, double, i8 }>,
//       <{ i64, i64, double, i8 }>* %tuple1, i32 0, i32 0
//   %count_star_val = load i64, i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   %0 = getelementptr %"class.impala::AggFnEvaluator"*,
//       %"class.impala::AggFnEvaluator"** %agg_fn_evals, i32 1
//   %agg_fn_eval =
//       load %"class.impala::AggFnEvaluator"*, %"class.impala::AggFnEvaluator"** %0
//   call void @UpdateSlot(%"class.impala::AggFnEvaluator"* %agg_fn_eval,
//       <{ i64, i64, double, i8 }>* %tuple1, %"class.impala::TupleRow"* %row)
//   %1 = getelementptr %"class.impala::AggFnEvaluator"*,
//       %"class.impala::AggFnEvaluator"** %agg_fn_evals, i32 2
//   %agg_fn_eval2 =
//       load %"class.impala::AggFnEvaluator"*, %"class.impala::AggFnEvaluator"** %1
//   call void @UpdateSlot.2(%"class.impala::AggFnEvaluator"* %agg_fn_eval2,
//       <{ i64, i64, double, i8 }>* %tuple1, %"class.impala::TupleRow"* %row)
//   ret void
// }
//
Status Aggregator::CodegenUpdateTuple(LlvmCodeGen* codegen, llvm::Function** fn) {
  for (const SlotDescriptor* slot_desc : intermediate_tuple_desc_->slots()) {
    if (slot_desc->type().type == TYPE_CHAR) {
      return Status::Expected("Aggregator::CodegenUpdateTuple(): cannot "
                              "codegen CHAR in aggregations");
    }
  }

  if (intermediate_tuple_desc_->GetLlvmStruct(codegen) == NULL) {
    return Status::Expected("Aggregator::CodegenUpdateTuple(): failed to"
                            " generate intermediate tuple desc");
  }

  // Get the types to match the UpdateTuple signature
  llvm::PointerType* agg_node_ptr_type = codegen->GetStructPtrType<Aggregator>();
  llvm::PointerType* evals_type = codegen->GetStructPtrPtrType<AggFnEvaluator>();
  llvm::PointerType* tuple_ptr_type = codegen->GetStructPtrType<Tuple>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();

  llvm::StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  llvm::PointerType* tuple_ptr = codegen->GetPtrType(tuple_struct);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_fn_evals", evals_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_merge", codegen->bool_type()));

  LlvmBuilder builder(codegen->context());
  llvm::Value* args[5];
  *fn = prototype.GeneratePrototype(&builder, &args[0]);
  llvm::Value* agg_fn_evals_arg = args[1];
  llvm::Value* tuple_arg = args[2];
  llvm::Value* row_arg = args[3];

  // Cast the parameter types to the internal llvm runtime types.
  // TODO: get rid of this by using right type in function signature
  tuple_arg = builder.CreateBitCast(tuple_arg, tuple_ptr, "tuple");

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  int j = grouping_exprs_.size();
  for (int i = 0; i < agg_fns_.size(); ++i, ++j) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFn* agg_fn = agg_fns_[i];
    if (agg_fn->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      int field_idx = slot_desc->llvm_field_idx();
      llvm::Value* const_one = codegen->GetI64Constant(1);
      llvm::Value* slot_ptr =
          builder.CreateStructGEP(NULL, tuple_arg, field_idx, "src_slot");
      llvm::Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      llvm::Value* count_inc =
          builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      llvm::Function* update_slot_fn;
      RETURN_IF_ERROR(CodegenUpdateSlot(codegen, i, slot_desc, &update_slot_fn));

      // Load agg_fn_evals_[i]
      llvm::Value* agg_fn_eval_val =
          codegen->CodegenArrayAt(&builder, agg_fn_evals_arg, i, "agg_fn_eval");

      // Call UpdateSlot(agg_fn_evals_[i], tuple, row);
      llvm::Value* update_slot_args[] = {agg_fn_eval_val, tuple_arg, row_arg};
      builder.CreateCall(update_slot_fn, update_slot_args);
    }
  }
  builder.CreateRetVoid();

  // Avoid inlining big UpdateTuple function into outer loop - we're unlikely to get
  // any benefit from it since the function call overhead will be amortized.
  if (agg_fns_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  // CodegenProcessBatch() does the final optimizations.
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("Aggregator::CodegenUpdateTuple(): codegen'd "
                  "UpdateTuple() function failed verification, see log");
  }
  return Status::OK();
}
}
