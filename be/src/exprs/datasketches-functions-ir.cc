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

#include "exprs/datasketches-functions.h"

#include "exprs/datasketches-common.h"
#include "thirdparty/datasketches/hll.hpp"

namespace impala {

BigIntVal DataSketchesFunctions::DsHllEstimate(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  datasketches::hll_sketch sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
  if (!DeserializeHllSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
  return sketch.get_estimate();
}

}

