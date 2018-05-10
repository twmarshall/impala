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

#include "exec/grouping-aggregator.h"

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

/// The minimum reduction factor (input rows divided by output rows) to grow hash tables
/// in a streaming preaggregation, given that the hash tables are currently the given
/// size or above. The sizes roughly correspond to hash table sizes where the bucket
/// arrays will fit in  a cache level. Intuitively, we don't want the working set of the
/// aggregation to expand to the next level of cache unless we're reducing the input
/// enough to outweigh the increased memory latency we'll incur for each hash table
/// lookup.
///
/// Note that the current reduction achieved is not always a good estimate of the
/// final reduction. It may be biased either way depending on the ordering of the
/// input. If the input order is random, we will underestimate the final reduction
/// factor because the probability of a row having the same key as a previous row
/// increases as more input is processed.  If the input order is correlated with the
/// key, skew may bias the estimate. If high cardinality keys appear first, we
/// may overestimate and if low cardinality keys appear first, we underestimate.
/// To estimate the eventual reduction achieved, we estimate the final reduction
/// using the planner's estimated input cardinality and the assumption that input
/// is in a random order. This means that we assume that the reduction factor will
/// increase over time.
struct StreamingHtMinReductionEntry {
  // Use 'streaming_ht_min_reduction' if the total size of hash table bucket directories
  // in bytes is greater than this threshold.
  int min_ht_mem;
  // The minimum reduction factor to expand the hash tables.
  double streaming_ht_min_reduction;
};

// TODO: experimentally tune these values and also programmatically get the cache size
// of the machine that we're running on.
static const StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
    // Expand up to L2 cache always.
    {0, 0.0},
    // Expand into L3 cache if we look like we're getting some reduction.
    {256 * 1024, 1.1},
    // Expand into main memory if we're getting a significant reduction.
    {2 * 1024 * 1024, 2.0},
};

static const int STREAMING_HT_MIN_REDUCTION_SIZE =
    sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

GroupingAggregator::GroupingAggregator(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_row_desc_(intermediate_tuple_desc_, false),
    is_streaming_preagg_(tnode.agg_node.use_streaming_preaggregation),
    needs_serialize_(false),
    output_partition_(NULL),
    process_batch_fn_(NULL),
    process_batch_streaming_fn_(NULL),
    ht_resize_timer_(NULL),
    get_results_timer_(NULL),
    num_hash_buckets_(NULL),
    partitions_created_(NULL),
    max_partition_level_(NULL),
    num_row_repartitioned_(NULL),
    num_repartitions_(NULL),
    num_spilled_partitions_(NULL),
    largest_partition_percent_(NULL),
    streaming_timer_(NULL),
    num_passthrough_rows_(NULL),
    preagg_estimated_reduction_(NULL),
    preagg_streaming_ht_min_reduction_(NULL),
    estimated_input_cardinality_(tnode.agg_node.estimated_input_cardinality),
    partition_eos_(false),
    partition_pool_(new ObjectPool()) {
  DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
  if (is_streaming_preagg_) {
    DCHECK(conjunct_evals_.empty()) << "Preaggs have no conjuncts";
    DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
    DCHECK(limit_ == -1) << "Preaggs have no limits";
  }
}

Status GroupingAggregator::Init(const TPlanNode& tnode, RuntimeState* state) {
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

Status GroupingAggregator::Prepare(RuntimeState* state) {
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

void GroupingAggregator::Codegen(RuntimeState* state) {
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

Status GroupingAggregator::Open(RuntimeState* state) {
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

Status GroupingAggregator::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
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

Status GroupingAggregator::GetRowsFromPartition(
    RuntimeState* state, RowBatch* row_batch) {
  DCHECK(!row_batch->AtCapacity());
  if (output_iterator_.AtEnd()) {
    // Done with this partition, move onto the next one.
    if (output_partition_ != NULL) {
      output_partition_->Close(false);
      output_partition_ = NULL;
    }
    if (aggregated_partitions_.empty() && spilled_partitions_.empty()) {
      // No more partitions, all done.
      partition_eos_ = true;
      return Status::OK();
    }
    // Process next partition.
    RETURN_IF_ERROR(NextPartition());
    DCHECK(output_partition_ != NULL);
  }

  SCOPED_TIMER(get_results_timer_);

  // The output row batch may reference memory allocated by Serialize() or Finalize(),
  // allocating that memory directly from the row batch's pool means we can safely return
  // the batch.
  vector<ScopedResultsPool> allocate_from_batch_pool = ScopedResultsPool::Create(
      output_partition_->agg_fn_evals, row_batch->tuple_data_pool());
  int count = 0;
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());
  // Keeping returning rows from the current partition.
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
    // This loop can go on for a long time if the conjuncts are very selective. Do query
    // maintenance every N iterations.
    if ((count++ & (N - 1)) == 0) {
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }

    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    Tuple* output_tuple = GetOutputTuple(output_partition_->agg_fn_evals,
        intermediate_tuple, row_batch->tuple_data_pool());
    output_iterator_.Next();
    row->SetTuple(0, output_tuple);
    DCHECK_EQ(conjunct_evals_.size(), conjuncts_.size());
    if (ExecNode::EvalConjuncts(conjunct_evals_.data(), conjuncts_.size(), row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break;
    }
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  partition_eos_ = ReachedLimit();
  if (output_iterator_.AtEnd()) row_batch->MarkNeedsDeepCopy();

  return Status::OK();
}

bool GroupingAggregator::ShouldExpandPreaggHashTables() const {
  int64_t ht_mem = 0;
  int64_t ht_rows = 0;
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    HashTable* ht = hash_partitions_[i]->hash_tbl.get();
    ht_mem += ht->CurrentMemSize();
    ht_rows += ht->size();
  }

  // Need some rows in tables to have valid statistics.
  if (ht_rows == 0) return true;

  // Find the appropriate reduction factor in our table for the current hash table sizes.
  int cache_level = 0;
  while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE
      && ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
    ++cache_level;
  }

  // Compare the number of rows in the hash table with the number of input rows that
  // were aggregated into it. Exclude passed through rows from this calculation since
  // they were not in hash tables.
  const int64_t input_rows = children_[0]->rows_returned();
  const int64_t aggregated_input_rows = input_rows - num_rows_returned_;
  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
  double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

  // TODO: workaround for IMPALA-2490: subplan node rows_returned counter may be
  // inaccurate, which could lead to a divide by zero below.
  if (aggregated_input_rows <= 0) return true;

  // Extrapolate the current reduction factor (r) using the formula
  // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
  // set, N is the number of input rows, excluding passed-through rows, and n is the
  // number of rows inserted or merged into the hash tables. This is a very rough
  // approximation but is good enough to be useful.
  // TODO: consider collecting more statistics to better estimate reduction.
  double estimated_reduction = aggregated_input_rows >= expected_input_rows ?
      current_reduction :
      1 + (expected_input_rows / aggregated_input_rows) * (current_reduction - 1);
  double min_reduction =
      STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
  COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
  return estimated_reduction > min_reduction;
}

void GroupingAggregator::CleanupHashTbl(
    const vector<AggFnEvaluator*>& agg_fn_evals, HashTable::Iterator it) {
  if (!needs_finalize_ && !needs_serialize_) return;

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs.
  if (needs_finalize_) {
    // Finalize() requires a dst tuple but we don't actually need the result,
    // so allocate a single dummy tuple to avoid accumulating memory.
    Tuple* dummy_dst = NULL;
    dummy_dst =
        Tuple::Create(output_tuple_desc_->byte_size(), singleton_tuple_pool_.get());
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple();
      AggFnEvaluator::Finalize(agg_fn_evals, tuple, dummy_dst);
      it.Next();
      // Free any expr result allocations to prevent them accumulating excessively.
      expr_results_pool_->Clear();
    }
  } else {
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple();
      AggFnEvaluator::Serialize(agg_fn_evals, tuple);
      it.Next();
      // Free any expr result allocations to prevent them accumulating excessively.
      expr_results_pool_->Clear();
    }
  }
}

Status GroupingAggregator::Reset(RuntimeState* state) {
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

void GroupingAggregator::Close(RuntimeState* state) {
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

Tuple* GroupingAggregator::ConstructIntermediateTuple(
    const vector<AggFnEvaluator*>& agg_fn_evals, MemPool* pool, Status* status) noexcept {
  const int fixed_size = intermediate_tuple_desc_->byte_size();
  const int varlen_size = GroupingExprsVarlenSize();
  const int tuple_data_size = fixed_size + varlen_size;
  uint8_t* tuple_data = pool->TryAllocate(tuple_data_size);
  if (UNLIKELY(tuple_data == NULL)) {
    string details = Substitute("Cannot perform aggregation at node with id $0. Failed "
                                "to allocate $1 bytes for intermediate tuple.",
        id_, tuple_data_size);
    *status = pool->mem_tracker()->MemLimitExceeded(state_, details, tuple_data_size);
    return NULL;
  }
  memset(tuple_data, 0, fixed_size);
  Tuple* intermediate_tuple = reinterpret_cast<Tuple*>(tuple_data);
  uint8_t* varlen_data = tuple_data + fixed_size;
  CopyGroupingValues(intermediate_tuple, varlen_data, varlen_size);
  InitAggSlots(agg_fn_evals, intermediate_tuple);
  return intermediate_tuple;
}

Tuple* GroupingAggregator::ConstructIntermediateTuple(
    const vector<AggFnEvaluator*>& agg_fn_evals, BufferedTupleStream* stream,
    Status* status) noexcept {
  DCHECK(stream != NULL && status != NULL);
  // Allocate space for the entire tuple in the stream.
  const int fixed_size = intermediate_tuple_desc_->byte_size();
  const int varlen_size = GroupingExprsVarlenSize();
  const int tuple_size = fixed_size + varlen_size;
  uint8_t* tuple_data = stream->AddRowCustomBegin(tuple_size, status);
  if (UNLIKELY(tuple_data == nullptr)) {
    // If we failed to allocate and did not hit an error (indicated by a non-ok status),
    // the caller of this function can try to free some space, e.g. through spilling, and
    // re-attempt to allocate space for this row.
    return nullptr;
  }
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_data);
  tuple->Init(fixed_size);
  uint8_t* varlen_buffer = tuple_data + fixed_size;
  CopyGroupingValues(tuple, varlen_buffer, varlen_size);
  InitAggSlots(agg_fn_evals, tuple);
  stream->AddRowCustomEnd(tuple_size);
  return tuple;
}

int GroupingAggregator::GroupingExprsVarlenSize() {
  int varlen_size = 0;
  // TODO: The hash table could compute this as it hashes.
  for (int expr_idx : string_grouping_exprs_) {
    StringValue* sv = reinterpret_cast<StringValue*>(ht_ctx_->ExprValue(expr_idx));
    // Avoid branching by multiplying length by null bit.
    varlen_size += sv->len * !ht_ctx_->ExprValueNull(expr_idx);
  }
  return varlen_size;
}

// TODO: codegen this function.
void GroupingAggregator::CopyGroupingValues(
    Tuple* intermediate_tuple, uint8_t* buffer, int varlen_size) {
  // Copy over all grouping slots (the variable length data is copied below).
  for (int i = 0; i < grouping_exprs_.size(); ++i) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[i];
    if (ht_ctx_->ExprValueNull(i)) {
      intermediate_tuple->SetNull(slot_desc->null_indicator_offset());
    } else {
      void* src = ht_ctx_->ExprValue(i);
      void* dst = intermediate_tuple->GetSlot(slot_desc->tuple_offset());
      memcpy(dst, src, slot_desc->slot_size());
    }
  }

  for (int expr_idx : string_grouping_exprs_) {
    if (ht_ctx_->ExprValueNull(expr_idx)) continue;

    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[expr_idx];
    // ptr and len were already copied to the fixed-len part of string value
    StringValue* sv = reinterpret_cast<StringValue*>(
        intermediate_tuple->GetSlot(slot_desc->tuple_offset()));
    memcpy(buffer, sv->ptr, sv->len);
    sv->ptr = reinterpret_cast<char*>(buffer);
    buffer += sv->len;
  }
}

template <bool AGGREGATED_ROWS>
Status GroupingAggregator::AppendSpilledRow(
    Partition* __restrict__ partition, TupleRow* __restrict__ row) {
  DCHECK(!is_streaming_preagg_);
  DCHECK(partition->is_spilled());
  BufferedTupleStream* stream = AGGREGATED_ROWS ?
      partition->aggregated_row_stream.get() :
      partition->unaggregated_row_stream.get();
  DCHECK(!stream->is_pinned());
  Status status;
  if (LIKELY(stream->AddRow(row, &status))) return Status::OK();
  RETURN_IF_ERROR(status);

  // Keep trying to free memory by spilling until we succeed or hit an error.
  // Running out of partitions to spill is treated as an error by SpillPartition().
  while (true) {
    RETURN_IF_ERROR(SpillPartition(AGGREGATED_ROWS));
    if (stream->AddRow(row, &status)) return Status::OK();
    RETURN_IF_ERROR(status);
  }
}

string GroupingAggregator::DebugString(int indentation_level) const {
  stringstream ss;
  DebugString(indentation_level, &ss);
  return ss.str();
}

void GroupingAggregator::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "GroupingAggregator("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_ << " needs_finalize=" << needs_finalize_
       << " grouping_exprs=" << ScalarExpr::DebugString(grouping_exprs_)
       << " agg_exprs=" << AggFn::DebugString(agg_fns_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status GroupingAggregator::CreateHashPartitions(int level, int single_partition_idx) {
  if (is_streaming_preagg_) DCHECK_EQ(level, 0);
  if (UNLIKELY(level >= MAX_PARTITION_DEPTH)) {
    return Status(
        TErrorCode::PARTITIONED_AGG_MAX_PARTITION_DEPTH, id_, MAX_PARTITION_DEPTH);
  }
  ht_ctx_->set_level(level);

  DCHECK(hash_partitions_.empty());
  int num_partitions_created = 0;
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = nullptr;
    if (single_partition_idx == -1 || i == single_partition_idx) {
      Partition* new_partition = partition_pool_->Add(new Partition(this, level, i));
      ++num_partitions_created;
      hash_partitions_.push_back(new_partition);
      RETURN_IF_ERROR(new_partition->InitStreams());
    } else {
      hash_partitions_.push_back(nullptr);
    }
  }
  // Now that all the streams are reserved (meaning we have enough memory to execute
  // the algorithm), allocate the hash tables. These can fail and we can still continue.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition == nullptr) continue;
    if (partition->aggregated_row_stream == nullptr) {
      // Failed to create the aggregated row stream - cannot create a hash table.
      // Just continue with a NULL hash table so rows will be passed through.
      DCHECK(is_streaming_preagg_);
    } else {
      bool got_memory;
      RETURN_IF_ERROR(partition->InitHashTable(&got_memory));
      // Spill the partition if we cannot create a hash table for a merge aggregation.
      if (UNLIKELY(!got_memory)) {
        DCHECK(!is_streaming_preagg_) << "Preagg reserves enough memory for hash tables";
        // If we're repartitioning, we will be writing aggregated rows first.
        RETURN_IF_ERROR(partition->Spill(level > 0));
      }
    }
    hash_tbls_[i] = partition->hash_tbl.get();
  }
  // In this case we did not have to repartition, so ensure that while building the hash
  // table all rows will be inserted into the partition at 'single_partition_idx' in case
  // a non deterministic grouping expression causes a row to hash to a different
  // partition index.
  if (single_partition_idx != -1) {
    Partition* partition = hash_partitions_[single_partition_idx];
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      hash_partitions_[i] = partition;
      hash_tbls_[i] = partition->hash_tbl.get();
    }
  }

  COUNTER_ADD(partitions_created_, num_partitions_created);
  if (!is_streaming_preagg_) {
    COUNTER_SET(max_partition_level_, level);
  }
  return Status::OK();
}

Status GroupingAggregator::CheckAndResizeHashPartitions(
    bool partitioning_aggregated_rows, int num_rows, const HashTableCtx* ht_ctx) {
  DCHECK(!is_streaming_preagg_);
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition == nullptr) continue;
    while (!partition->is_spilled()) {
      {
        SCOPED_TIMER(ht_resize_timer_);
        bool resized;
        RETURN_IF_ERROR(partition->hash_tbl->CheckAndResize(num_rows, ht_ctx, &resized));
        if (resized) break;
      }
      RETURN_IF_ERROR(SpillPartition(partitioning_aggregated_rows));
    }
  }
  return Status::OK();
}

Status GroupingAggregator::NextPartition() {
  DCHECK(output_partition_ == nullptr);

  if (!IsInSubplan() && spilled_partitions_.empty()) {
    // All partitions are in memory. Release reservation that was used for previous
    // partitions that is no longer needed. If we have spilled partitions, we want to
    // hold onto all reservation in case it is needed to process the spilled partitions.
    DCHECK(!buffer_pool_client()->has_unpinned_pages());
    Status status = ReleaseUnusedReservation();
    DCHECK(status.ok()) << "Should not fail - all partitions are in memory so there are "
                        << "no unpinned pages. " << status.GetDetail();
  }

  // Keep looping until we get to a partition that fits in memory.
  Partition* partition = nullptr;
  while (true) {
    // First return partitions that are fully aggregated (and in memory).
    if (!aggregated_partitions_.empty()) {
      partition = aggregated_partitions_.front();
      DCHECK(!partition->is_spilled());
      aggregated_partitions_.pop_front();
      break;
    }

    // No aggregated partitions in memory - we should not be using any reservation aside
    // from 'serialize_stream_'.
    DCHECK_EQ(serialize_stream_ != nullptr ? serialize_stream_->BytesPinned(false) : 0,
        buffer_pool_client()->GetUsedReservation())
        << buffer_pool_client()->DebugString();

    // Try to fit a single spilled partition in memory. We can often do this because
    // we only need to fit 1/PARTITION_FANOUT of the data in memory.
    // TODO: in some cases when the partition probably won't fit in memory it could
    // be better to skip directly to repartitioning.
    RETURN_IF_ERROR(BuildSpilledPartition(&partition));
    if (partition != nullptr) break;

    // If we can't fit the partition in memory, repartition it.
    RETURN_IF_ERROR(RepartitionSpilledPartition());
  }
  DCHECK(!partition->is_spilled());
  DCHECK(partition->hash_tbl.get() != nullptr);
  DCHECK(partition->aggregated_row_stream->is_pinned());

  output_partition_ = partition;
  output_iterator_ = output_partition_->hash_tbl->Begin(ht_ctx_.get());
  COUNTER_ADD(num_hash_buckets_, output_partition_->hash_tbl->num_buckets());
  return Status::OK();
}

Status GroupingAggregator::BuildSpilledPartition(Partition** built_partition) {
  DCHECK(!spilled_partitions_.empty());
  DCHECK(!is_streaming_preagg_);
  // Leave the partition in 'spilled_partitions_' to be closed if we hit an error.
  Partition* src_partition = spilled_partitions_.front();
  DCHECK(src_partition->is_spilled());

  // Create a new hash partition from the rows of the spilled partition. This is simpler
  // than trying to finish building a partially-built partition in place. We only
  // initialise one hash partition that all rows in 'src_partition' will hash to.
  RETURN_IF_ERROR(CreateHashPartitions(src_partition->level, src_partition->idx));
  Partition* dst_partition = hash_partitions_[src_partition->idx];
  DCHECK(dst_partition != nullptr);

  // Rebuild the hash table over spilled aggregate rows then start adding unaggregated
  // rows to the hash table. It's possible the partition will spill at either stage.
  // In that case we need to finish processing 'src_partition' so that all rows are
  // appended to 'dst_partition'.
  // TODO: if the partition spills again but the aggregation reduces the input
  // significantly, we could do better here by keeping the incomplete hash table in
  // memory and only spilling unaggregated rows that didn't fit in the hash table
  // (somewhat similar to the passthrough pre-aggregation).
  RETURN_IF_ERROR(ProcessStream<true>(src_partition->aggregated_row_stream.get()));
  RETURN_IF_ERROR(ProcessStream<false>(src_partition->unaggregated_row_stream.get()));
  src_partition->Close(false);
  spilled_partitions_.pop_front();
  hash_partitions_.clear();

  if (dst_partition->is_spilled()) {
    PushSpilledPartition(dst_partition);
    *built_partition = nullptr;
    // Spilled the partition - we should not be using any reservation except from
    // 'serialize_stream_'.
    DCHECK_EQ(serialize_stream_ != nullptr ? serialize_stream_->BytesPinned(false) : 0,
        buffer_pool_client()->GetUsedReservation())
        << buffer_pool_client()->DebugString();
  } else {
    *built_partition = dst_partition;
  }
  return Status::OK();
}

Status GroupingAggregator::RepartitionSpilledPartition() {
  DCHECK(!spilled_partitions_.empty());
  DCHECK(!is_streaming_preagg_);
  // Leave the partition in 'spilled_partitions_' to be closed if we hit an error.
  Partition* partition = spilled_partitions_.front();
  DCHECK(partition->is_spilled());

  // Create the new hash partitions to repartition into. This will allocate a
  // write buffer for each partition's aggregated row stream.
  RETURN_IF_ERROR(CreateHashPartitions(partition->level + 1));
  COUNTER_ADD(num_repartitions_, 1);

  // Rows in this partition could have been spilled into two streams, depending
  // on if it is an aggregated intermediate, or an unaggregated row. Aggregated
  // rows are processed first to save a hash table lookup in ProcessBatch().
  RETURN_IF_ERROR(ProcessStream<true>(partition->aggregated_row_stream.get()));

  // Prepare write buffers so we can append spilled rows to unaggregated partitions.
  for (Partition* hash_partition : hash_partitions_) {
    if (!hash_partition->is_spilled()) continue;
    // The aggregated rows have been repartitioned. Free up at least a buffer's worth of
    // reservation and use it to pin the unaggregated write buffer.
    hash_partition->aggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL);
    bool got_buffer;
    RETURN_IF_ERROR(
        hash_partition->unaggregated_row_stream->PrepareForWrite(&got_buffer));
    DCHECK(got_buffer) << "Accounted in min reservation"
                       << buffer_pool_client()->DebugString();
  }
  RETURN_IF_ERROR(ProcessStream<false>(partition->unaggregated_row_stream.get()));

  COUNTER_ADD(num_row_repartitioned_, partition->aggregated_row_stream->num_rows());
  COUNTER_ADD(num_row_repartitioned_, partition->unaggregated_row_stream->num_rows());

  partition->Close(false);
  spilled_partitions_.pop_front();

  // Done processing this partition. Move the new partitions into
  // spilled_partitions_/aggregated_partitions_.
  int64_t num_input_rows = partition->aggregated_row_stream->num_rows()
      + partition->unaggregated_row_stream->num_rows();
  RETURN_IF_ERROR(MoveHashPartitions(num_input_rows));
  return Status::OK();
}

template <bool AGGREGATED_ROWS>
Status GroupingAggregator::ProcessStream(BufferedTupleStream* input_stream) {
  DCHECK(!is_streaming_preagg_);
  if (input_stream->num_rows() > 0) {
    while (true) {
      bool got_buffer = false;
      RETURN_IF_ERROR(input_stream->PrepareForRead(true, &got_buffer));
      if (got_buffer) break;
      // Did not have a buffer to read the input stream. Spill and try again.
      RETURN_IF_ERROR(SpillPartition(AGGREGATED_ROWS));
    }

    TPrefetchMode::type prefetch_mode = state_->query_options().prefetch_mode;
    bool eos = false;
    const RowDescriptor* desc =
        AGGREGATED_ROWS ? &intermediate_row_desc_ : children_[0]->row_desc();
    RowBatch batch(desc, state_->batch_size(), mem_tracker());
    do {
      RETURN_IF_ERROR(input_stream->GetNext(&batch, &eos));
      RETURN_IF_ERROR(
          ProcessBatch<AGGREGATED_ROWS>(&batch, prefetch_mode, ht_ctx_.get()));
      RETURN_IF_ERROR(QueryMaintenance(state_));
      batch.Reset();
    } while (!eos);
  }
  input_stream->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  return Status::OK();
}

Status GroupingAggregator::SpillPartition(bool more_aggregate_rows) {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick the largest partition that is not spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i] == nullptr) continue;
    if (hash_partitions_[i]->is_closed) continue;
    if (hash_partitions_[i]->is_spilled()) continue;
    // Pass 'true' because we need to keep the write block pinned. See Partition::Spill().
    int64_t mem = hash_partitions_[i]->aggregated_row_stream->BytesPinned(true);
    mem += hash_partitions_[i]->hash_tbl->ByteSize();
    mem += hash_partitions_[i]->agg_fn_perm_pool->total_reserved_bytes();
    DCHECK_GT(mem, 0); // At least the hash table buckets should occupy memory.
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }
  DCHECK_NE(partition_idx, -1) << "Should have been able to spill a partition to "
                               << "reclaim memory: "
                               << buffer_pool_client()->DebugString();
  // Remove references to the destroyed hash table from 'hash_tbls_'.
  // Additionally, we might be dealing with a rebuilt spilled partition, where all
  // partitions point to a single in-memory partition. This also ensures that 'hash_tbls_'
  // remains consistent in that case.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    if (hash_partitions_[i] == hash_partitions_[partition_idx]) hash_tbls_[i] = nullptr;
  }
  return hash_partitions_[partition_idx]->Spill(more_aggregate_rows);
}

Status GroupingAggregator::MoveHashPartitions(int64_t num_input_rows) {
  DCHECK(!hash_partitions_.empty());
  stringstream ss;
  ss << "PA(node_id=" << id() << ") partitioned(level=" << hash_partitions_[0]->level
     << ") " << num_input_rows << " rows into:" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition == nullptr) continue;
    // We might be dealing with a rebuilt spilled partition, where all partitions are
    // pointing to a single in-memory partition, so make sure we only proceed for the
    // right partition.
    if (i != partition->idx) continue;
    int64_t aggregated_rows = 0;
    if (partition->aggregated_row_stream != nullptr) {
      aggregated_rows = partition->aggregated_row_stream->num_rows();
    }
    int64_t unaggregated_rows = 0;
    if (partition->unaggregated_row_stream != nullptr) {
      unaggregated_rows = partition->unaggregated_row_stream->num_rows();
    }
    double total_rows = aggregated_rows + unaggregated_rows;
    double percent = total_rows * 100 / num_input_rows;
    ss << "  " << i << " " << (partition->is_spilled() ? "spilled" : "not spilled")
       << " (fraction=" << fixed << setprecision(2) << percent << "%)" << endl
       << "    #aggregated rows:" << aggregated_rows << endl
       << "    #unaggregated rows: " << unaggregated_rows << endl;

    // TODO: update counters to support doubles.
    COUNTER_SET(largest_partition_percent_, static_cast<int64_t>(percent));

    if (total_rows == 0) {
      partition->Close(false);
    } else if (partition->is_spilled()) {
      PushSpilledPartition(partition);
    } else {
      aggregated_partitions_.push_back(partition);
    }
  }
  VLOG(2) << ss.str();
  hash_partitions_.clear();
  return Status::OK();
}

void GroupingAggregator::PushSpilledPartition(Partition* partition) {
  DCHECK(partition->is_spilled());
  DCHECK(partition->hash_tbl == nullptr);
  // Ensure all pages in the spilled partition's streams are unpinned by invalidating
  // the streams' read and write iterators. We may need all the memory to process the
  // next spilled partitions.
  partition->aggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL);
  partition->unaggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL);
  spilled_partitions_.push_front(partition);
}

void GroupingAggregator::ClosePartitions() {
  for (Partition* partition : hash_partitions_) {
    if (partition != nullptr) partition->Close(true);
  }
  hash_partitions_.clear();
  for (Partition* partition : aggregated_partitions_) partition->Close(true);
  aggregated_partitions_.clear();
  for (Partition* partition : spilled_partitions_) partition->Close(true);
  spilled_partitions_.clear();
  memset(hash_tbls_, 0, sizeof(hash_tbls_));
  partition_pool_->Clear();
}

Status GroupingAggregator::CodegenProcessBatch(
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
    return Status("GroupingAggregator::CodegenProcessBatch(): codegen'd "
                  "ProcessBatch() function failed verification, see log");
  }

  void** codegened_fn_ptr = grouping_exprs_.empty() ?
      reinterpret_cast<void**>(&process_batch_no_grouping_fn_) :
      reinterpret_cast<void**>(&process_batch_fn_);
  codegen->AddFunctionToJit(process_batch_fn, codegened_fn_ptr);
  return Status::OK();
}

Status GroupingAggregator::CodegenProcessBatchStreaming(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  DCHECK(is_streaming_preagg_);

  IRFunction::Type ir_fn = IRFunction::PART_AGG_NODE_PROCESS_BATCH_STREAMING;
  llvm::Function* process_batch_streaming_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(process_batch_streaming_fn != NULL);

  // Make needs_serialize arg constant so dead code can be optimised out.
  llvm::Value* needs_serialize_arg = codegen->GetArgument(process_batch_streaming_fn, 2);
  needs_serialize_arg->replaceAllUsesWith(codegen->GetBoolConstant(needs_serialize_));

  // Replace prefetch_mode with constant so branches can be optimised out.
  llvm::Value* prefetch_mode_arg = codegen->GetArgument(process_batch_streaming_fn, 3);
  prefetch_mode_arg->replaceAllUsesWith(codegen->GetI32Constant(prefetch_mode));

  llvm::Function* update_tuple_fn;
  RETURN_IF_ERROR(CodegenUpdateTuple(codegen, &update_tuple_fn));

  // We only use the top-level hash function for streaming aggregations.
  llvm::Function* hash_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, false, &hash_fn));

  // Codegen HashTable::Equals
  llvm::Function* equals_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenEquals(codegen, true, &equals_fn));

  // Codegen for evaluating input rows
  llvm::Function* eval_grouping_expr_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(codegen, false, &eval_grouping_expr_fn));

  // Replace call sites
  int replaced = codegen->ReplaceCallSites(
      process_batch_streaming_fn, update_tuple_fn, "UpdateTuple");
  DCHECK_EQ(replaced, 2);

  replaced = codegen->ReplaceCallSites(
      process_batch_streaming_fn, eval_grouping_expr_fn, "EvalProbeRow");
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, hash_fn, "HashRow");
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_batch_streaming_fn, equals_fn, "Equals");
  DCHECK_EQ(replaced, 1);

  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = false;
  RETURN_IF_ERROR(ht_ctx_->ReplaceHashTableConstants(
      codegen, stores_duplicates, 1, process_batch_streaming_fn, &replaced_constants));
  DCHECK_GE(replaced_constants.stores_nulls, 1);
  DCHECK_GE(replaced_constants.finds_some_nulls, 1);
  DCHECK_GE(replaced_constants.stores_duplicates, 1);
  DCHECK_GE(replaced_constants.stores_tuples, 1);
  DCHECK_GE(replaced_constants.quadratic_probing, 1);

  DCHECK(process_batch_streaming_fn != NULL);
  process_batch_streaming_fn = codegen->FinalizeFunction(process_batch_streaming_fn);
  if (process_batch_streaming_fn == NULL) {
    return Status("GroupingAggregator::CodegenProcessBatchStreaming(): codegen'd "
                  "ProcessBatchStreaming() function failed verification, see log");
  }

  codegen->AddFunctionToJit(
      process_batch_streaming_fn, reinterpret_cast<void**>(&process_batch_streaming_fn_));
  return Status::OK();
}

// Instantiate required templates.
template Status GroupingAggregator::AppendSpilledRow<false>(Partition*, TupleRow*);
template Status GroupingAggregator::AppendSpilledRow<true>(Partition*, TupleRow*);
} // namespace impala
