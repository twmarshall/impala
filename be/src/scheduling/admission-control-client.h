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

#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/common.pb.h"
#include "scheduling/admission-controller.h"

namespace impala {

// Used to abstract out the logic for submitting queries to an admission controller
// running locally or to one running remotely.
class AdmissionControlClient {
 public:
  AdmissionControlClient(const TUniqueId& query_id);

  Status SubmitForAdmission(const AdmissionController::AdmissionRequest& request,
      std::unique_ptr<QuerySchedulePB>* schedule_result);

  void ReleaseQuery(int64_t peak_mem_consumption);

  void ReleaseQueryBackends(const std::vector<NetworkAddressPB>& host_addr);

  void CancelAdmission();

 private:
  UniqueIdPB query_id_;

  /// True if this client should submit queries to the admission controller running
  /// in the local impalad.
  bool use_local_;

  /// Promise used by the admission controller. AdmissionController:AdmitQuery() will
  /// block on this promise until the query is either rejected, admitted, times out, or is
  /// cancelled. Can be set to CANCELLED by the ClientRequestState in order to cancel, but
  /// otherwise is set by AdmissionController with the admission decision.
  Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome_;
};

} // namespace impala
