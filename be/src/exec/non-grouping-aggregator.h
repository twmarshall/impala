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

#ifndef IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H
#define IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H

#include <memory>
#include <vector>

#include "exec/aggregator.h"
#include "runtime/mem-pool.h"

namespace impala {

class AggFnEvaluator;
class DescriptorTbl;
class ExecNode;
class LlvmCodeGen;
class ObjectPool;
class RowBatch;
class RuntimeState;
class TPlanNode;
class Tuple;

class NonGroupingAggregator : public Aggregator {
 public:
  NonGroupingAggregator(
      ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state) override;
  virtual Status Prepare(RuntimeState* state) override;
  virtual void Codegen(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state) override;
  virtual void Close(RuntimeState* state) override;

  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

 private:
  /// MemPool used to allocate memory for when we don't have grouping and don't initialize
  /// the partitioning structures, or during Close() when creating new output tuples.
  /// For non-grouping aggregations, the ownership of the pool's memory is transferred
  /// to the output batch on eos. The pool should not be Reset() to allow amortizing
  /// memory allocation over a series of Reset()/Open()/GetNext()* calls.
  std::unique_ptr<MemPool> singleton_tuple_pool_;

  typedef Status (*ProcessBatchNoGroupingFn)(NonGroupingAggregator*, RowBatch*);
  /// Jitted ProcessBatchNoGrouping function pointer. Null if codegen is disabled.
  ProcessBatchNoGroupingFn process_batch_no_grouping_fn_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Result of aggregation w/o GROUP BY.
  /// Note: can be NULL even if there is no grouping if the result tuple is 0 width
  /// e.g. select 1 from table group by col.
  Tuple* singleton_output_tuple_;
  bool singleton_output_tuple_returned_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Constructs singleton output tuple, allocating memory from pool.
  Tuple* ConstructSingletonOutputTuple(
      const std::vector<AggFnEvaluator*>& agg_fn_evals, MemPool* pool);

  /// Do the aggregation for all tuple rows in the batch when there is no grouping.
  /// This function is replaced by codegen.
  Status ProcessBatchNoGrouping(RowBatch* batch) WARN_UNUSED_RESULT;

  /// Output 'singleton_output_tuple_' and transfer memory to 'row_batch'.
  void GetSingletonOutput(RowBatch* row_batch);

  /// Codegen the non-streaming process row batch loop. The loop has already been
  /// compiled to IR and loaded into the codegen object. UpdateAggTuple has also been
  /// codegen'd to IR. This function will modify the loop subsituting the statically
  /// compiled functions with codegen'd ones. 'process_batch_fn_' or
  /// 'process_batch_no_grouping_fn_' will be updated with the codegened function
  /// depending on whether this is a grouping or non-grouping aggregation.
  /// Assumes AGGREGATED_ROWS = false.
  Status CodegenProcessBatch(
      LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) WARN_UNUSED_RESULT;
};
} // namespace impala

#endif // IMPALA_EXEC_NON_GROUPING_AGGREGATOR_H
