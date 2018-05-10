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

#include "exec/non-grouping-aggregator.h"

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

NonGroupingAggregator::NonGroupingAggregator(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    process_batch_no_grouping_fn_(NULL),
    singleton_output_tuple_(NULL),
    singleton_output_tuple_returned_(true) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
  if (is_streaming_preagg_) {
    DCHECK(conjunct_evals_.empty()) << "Preaggs have no conjuncts";
    DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
    DCHECK(limit_ == -1) << "Preaggs have no limits";
  }
}

Status NonGroupingAggregator::Init(const TPlanNode& tnode, RuntimeState* state) {
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

Status NonGroupingAggregator::Prepare(RuntimeState* state) {
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

void NonGroupingAggregator::Codegen(RuntimeState* state) {
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

Status NonGroupingAggregator::Open(RuntimeState* state) {
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

Status NonGroupingAggregator::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
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

void NonGroupingAggregator::GetSingletonOutput(RowBatch* row_batch) {
  DCHECK(grouping_exprs_.empty());
  int row_idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(row_idx);
  // The output row batch may reference memory allocated by Serialize() or Finalize(),
  // allocating that memory directly from the row batch's pool means we can safely return
  // the batch.
  vector<ScopedResultsPool> allocate_from_batch_pool =
      ScopedResultsPool::Create(agg_fn_evals_, row_batch->tuple_data_pool());
  Tuple* output_tuple = GetOutputTuple(
      agg_fn_evals_, singleton_output_tuple_, row_batch->tuple_data_pool());
  row->SetTuple(0, output_tuple);
  if (ExecNode::EvalConjuncts(conjunct_evals_.data(), conjunct_evals_.size(), row)) {
    row_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  // Keep the current chunk to amortize the memory allocation over a series
  // of Reset()/Open()/GetNext()* calls.
  row_batch->tuple_data_pool()->AcquireData(singleton_tuple_pool_.get(), true);
  // This node no longer owns the memory for singleton_output_tuple_.
  singleton_output_tuple_ = NULL;
}

Status NonGroupingAggregator::Reset(RuntimeState* state) {
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

void NonGroupingAggregator::Close(RuntimeState* state) {
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

Tuple* NonGroupingAggregator::ConstructSingletonOutputTuple(
    const vector<AggFnEvaluator*>& agg_fn_evals, MemPool* pool) {
  DCHECK(grouping_exprs_.empty());
  Tuple* output_tuple = Tuple::Create(intermediate_tuple_desc_->byte_size(), pool);
  InitAggSlots(agg_fn_evals, output_tuple);
  return output_tuple;
}

string NonGroupingAggregator::DebugString(int indentation_level) const {
  stringstream ss;
  DebugString(indentation_level, &ss);
  return ss.str();
}

void NonGroupingAggregator::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "NonGroupingAggregator("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_ << " needs_finalize=" << needs_finalize_
       << " grouping_exprs=" << ScalarExpr::DebugString(grouping_exprs_)
       << " agg_exprs=" << AggFn::DebugString(agg_fns_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status NonGroupingAggregator::CodegenProcessBatch(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  llvm::Function* update_tuple_fn;
  RETURN_IF_ERROR(CodegenUpdateTuple(codegen, &update_tuple_fn));

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn =
      (!grouping_exprs_.empty() ? IRFunction::PART_AGG_NODE_PROCESS_BATCH_UNAGGREGATED :
                                  IRFunction::PART_AGG_NODE_PROCESS_BATCH_NO_GROUPING);
  llvm::Function* process_batch_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(process_batch_fn != NULL);

  int replaced;
  if (!grouping_exprs_.empty()) {
    // Codegen for grouping using hash table

    // Replace prefetch_mode with constant so branches can be optimised out.
    llvm::Value* prefetch_mode_arg = codegen->GetArgument(process_batch_fn, 3);
    prefetch_mode_arg->replaceAllUsesWith(codegen->GetI32Constant(prefetch_mode));

    // The codegen'd ProcessBatch function is only used in Open() with level_ = 0,
    // so don't use murmur hash
    llvm::Function* hash_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, /* use murmur */ false, &hash_fn));

    // Codegen HashTable::Equals<true>
    llvm::Function* build_equals_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenEquals(codegen, true, &build_equals_fn));

    // Codegen for evaluating input rows
    llvm::Function* eval_grouping_expr_fn;
    RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(codegen, false, &eval_grouping_expr_fn));

    // Replace call sites
    replaced = codegen->ReplaceCallSites(
        process_batch_fn, eval_grouping_expr_fn, "EvalProbeRow");
    DCHECK_EQ(replaced, 1);

    replaced = codegen->ReplaceCallSites(process_batch_fn, hash_fn, "HashRow");
    DCHECK_EQ(replaced, 1);

    replaced = codegen->ReplaceCallSites(process_batch_fn, build_equals_fn, "Equals");
    DCHECK_EQ(replaced, 1);

    HashTableCtx::HashTableReplacedConstants replaced_constants;
    const bool stores_duplicates = false;
    RETURN_IF_ERROR(ht_ctx_->ReplaceHashTableConstants(
        codegen, stores_duplicates, 1, process_batch_fn, &replaced_constants));
    DCHECK_GE(replaced_constants.stores_nulls, 1);
    DCHECK_GE(replaced_constants.finds_some_nulls, 1);
    DCHECK_GE(replaced_constants.stores_duplicates, 1);
    DCHECK_GE(replaced_constants.stores_tuples, 1);
    DCHECK_GE(replaced_constants.quadratic_probing, 1);
  }

  replaced = codegen->ReplaceCallSites(process_batch_fn, update_tuple_fn, "UpdateTuple");
  DCHECK_GE(replaced, 1);
  process_batch_fn = codegen->FinalizeFunction(process_batch_fn);
  if (process_batch_fn == NULL) {
    return Status("NonGroupingAggregator::CodegenProcessBatch(): codegen'd "
                  "ProcessBatch() function failed verification, see log");
  }

  void** codegened_fn_ptr = grouping_exprs_.empty() ?
      reinterpret_cast<void**>(&process_batch_no_grouping_fn_) :
      reinterpret_cast<void**>(&process_batch_fn_);
  codegen->AddFunctionToJit(process_batch_fn, codegened_fn_ptr);
  return Status::OK();
}
}
