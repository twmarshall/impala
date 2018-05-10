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

#ifndef IMPALA_EXEC_AGGREGATION_NODE_H
#define IMPALA_EXEC_AGGREGATION_NODE_H

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

class AggregationNode : public ExecNode {
 public:
  AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};
} // namespace impala

#endif
