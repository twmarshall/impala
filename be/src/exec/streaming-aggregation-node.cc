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

#include "exec/streaming-aggregation-node.h"

#include <sstream>

#include "gutil/strings/substitute.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

StreamingAggregationNode::StreamingAggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : AggregationNodeBase(pool, tnode, descs),
    child_eos_(false),
    replicate_agg_idx_(0),
    child_batch_eos_(true) {
  DCHECK(tnode.conjuncts.empty()) << "Preaggs have no conjuncts";
  DCHECK(limit_ == -1) << "Preaggs have no limits";
  for (int i = 0; i < tnode.agg_node.aggregators.size(); ++i) {
    DCHECK(tnode.agg_node.aggregators[i].use_streaming_preaggregation);
  }
}

Status StreamingAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));

  for (auto& agg : aggs_) RETURN_IF_ERROR(agg->Open(state));

  // Streaming preaggregations do all processing in GetNext().
  return Status::OK();
}

Status StreamingAggregationNode::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  // With multiple Aggregators, each will only set a single tuple per row. We rely on the
  // other tuples to be null to detect which Aggregator set which row.
  if (aggs_.size() > 1) row_batch->Clear();

  bool aggregator_eos = false;
  if (!child_eos_ || !child_batch_eos_) {
    // For streaming preaggregations, we process rows from the child as we go.
    RETURN_IF_ERROR(GetRowsStreaming(state, row_batch));
  } else {
    for (int i = 0; i < aggs_.size(); ++i) {
      if (!aggs_[i]->eos()) {
        bool aggregator_eos2 = false;
        RETURN_IF_ERROR(aggs_[i]->GetNext(state, row_batch, &aggregator_eos2));
        break;
      } else if (i == aggs_.size() - 1) {
        aggregator_eos = true;
      }
    }
  }

  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  *eos = aggregator_eos && child_eos_;
  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status StreamingAggregationNode::GetRowsStreaming(
    RuntimeState* state, RowBatch* out_batch) {
  if (child_batch_ == nullptr) {
    child_batch_.reset(
        new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  }

  // Create mini batches.
  vector<unique_ptr<RowBatch>> mini_batches;
  for (int i = 0; i < aggs_.size(); ++i) {
    unique_ptr<RowBatch> mini_batch(
        new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
    mini_batches.push_back(std::move(mini_batch));
  }

  do {
    DCHECK_EQ(out_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);

    if (child_batch_eos_) {
      DCHECK_EQ(child_batch_->num_rows(), 0);
      RETURN_IF_ERROR(child(0)->GetNext(state, child_batch_.get(), &child_eos_));
      child_batch_eos_ = false;
    }

    if (aggs_.size() == 1) {
      RETURN_IF_ERROR(aggs_[0]->AddBatchStreaming(
          state, out_batch, child_batch_.get(), &child_batch_eos_));
      if (child_batch_eos_) {
        child_batch_->Reset();
      }
      continue;
    }

    if (replicate_input_) {
      bool eos = false;
      while (replicate_agg_idx_ < aggs_.size()) {
        RETURN_IF_ERROR(aggs_[replicate_agg_idx_]->AddBatchStreaming(
            state, out_batch, child_batch_.get(), &eos));
        if (eos) ++replicate_agg_idx_;
        if (out_batch->AtCapacity()) break;
        DCHECK(eos);
      }
      if (replicate_agg_idx_ == aggs_.size() && eos) {
        replicate_agg_idx_ = 0;
        child_batch_eos_ = true;
        child_batch_->Reset();
      }
      continue;
    }

    // Separate input batch into mini batches destined for the different aggs.
    DCHECK_EQ(aggs_.size(), child(0)->row_desc()->tuple_descriptors().size());
    int num_tuples = child(0)->row_desc()->tuple_descriptors().size();
    int num_rows = child_batch_->num_rows();
    if (num_rows > 0) {
      RETURN_IF_ERROR(SplitMiniBatches(child_batch_.get(), &mini_batches));

      for (int i = 0; i < num_tuples; ++i) {
        RowBatch* mini_batch = mini_batches[i].get();
        if (mini_batch->num_rows() > 0) {
          bool eos;
          RETURN_IF_ERROR(
              aggs_[i]->AddBatchStreaming(state, out_batch, mini_batch, &eos));
          DCHECK(eos);
          mini_batch->Reset();
        }
      }
    }
    child_batch_eos_ = true;
    child_batch_->Reset();
  } while (out_batch->num_rows() == 0 && !child_eos_);

  if (child_eos_) {
    child(0)->Close(state);
    child_batch_.reset();
    for (auto& agg : aggs_) RETURN_IF_ERROR(agg->InputDone());
  }

  return Status::OK();
}

Status StreamingAggregationNode::Reset(RuntimeState* state) {
  DCHECK(false) << "Cannot reset preaggregation";
  return Status("Cannot reset preaggregation");
}

void StreamingAggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // All expr mem allocations should happen in the Aggregator.
  DCHECK(expr_results_pool() == nullptr
      || expr_results_pool()->total_allocated_bytes() == 0);
  for (auto& agg : aggs_) agg->Close(state);
  child_batch_.reset();
  ExecNode::Close(state);
}

void StreamingAggregationNode::DebugString(
    int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "StreamingAggregationNode(";
  for (auto& agg : aggs_) agg->DebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
} // namespace impala
