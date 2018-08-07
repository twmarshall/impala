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

#ifndef IMPALA_EXEC_AGGREGATION_NODE_BASE_H
#define IMPALA_EXEC_AGGREGATION_NODE_BASE_H

#include <memory>

#include "exec/aggregator.h"
#include "exec/exec-node.h"

namespace impala {

/// Base class containing common code for the ExecNodes that do aggregation,
/// AggregationNode and StreamingAggregationNode.
class AggregationNodeBase : public ExecNode {
 public:
  AggregationNodeBase(
      ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state) override;
  virtual Status Prepare(RuntimeState* state) override;
  virtual void Codegen(RuntimeState* state) override;

 protected:
  /// If true, the input to this node should be passed into each Aggregator in 'aggs_'.
  /// Otherwise, the input should be divided between the Aggregators using
  /// AggregationNodeBase::SplitMiniBatches().
  bool replicate_input_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Performs the actual work of aggregating input rows.
  std::vector<std::unique_ptr<Aggregator>> aggs_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Splits the rows of 'batch' up according to which tuple of the row is non-null such
  /// that a row with tuple 'i' non-null is copied into the batch 'mini_batches[i]'.
  /// It is expected that all rows of 'batch' have exactly 1 non-null tuple.
  Status SplitMiniBatches(
      RowBatch* batch, std::vector<std::unique_ptr<RowBatch>>* mini_batches);
};
} // namespace impala

#endif // IMPALA_EXEC_AGGREGATION_NODE_BASE_H
