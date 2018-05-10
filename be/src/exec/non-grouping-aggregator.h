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

#include <deque>

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/descriptors.h" // for TupleId
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace llvm {
class BasicBlock;
class Function;
class Value;
} // namespace llvm

namespace impala {

class AggFn;
class AggFnEvaluator;
class CodegenAnyVal;
class LlvmCodeGen;
class LlvmBuilder;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;
class SlotDescriptor;

class NonGroupingAggregator : public Aggregator {
 public:
  NonGroupingAggregator(
      ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /// MemPool used to allocate memory for when we don't have grouping and don't initialize
  /// the partitioning structures, or during Close() when creating new output tuples.
  /// For non-grouping aggregations, the ownership of the pool's memory is transferred
  /// to the output batch on eos. The pool should not be Reset() to allow amortizing
  /// memory allocation over a series of Reset()/Open()/GetNext()* calls.
  boost::scoped_ptr<MemPool> singleton_tuple_pool_;

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

#endif
