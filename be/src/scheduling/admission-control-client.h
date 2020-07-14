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

#include <list>
#include <string>
#include <utility>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <gtest/gtest_prod.h>

#include "common/status.h"
#include "scheduling/admission-controller.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/schedule-state.h"
#include "statestore/statestore-subscriber.h"
#include "util/condition-variable.h"
#include "util/internal-queue.h"
#include "util/runtime-profile.h"

namespace impala {

// Used to abstract out the logic for submitting queries to an admission controller
// running locally or to one running remotely.
class AdmissionControlClient {
 public:
  AdmissionControlClient(const TUniqueId& query_id);

  /// Submits the query for admission and blocks until a decision is made, either by
  /// calling SubmitForAdmission() on a local AdmissionController or by sending an
  /// AdmitQueryRequest rpc and then repeatedly calling GetQueryStatus().
  Status SubmitForAdmission(const AdmissionController::AdmissionRequest& request,
      std::unique_ptr<QuerySchedulePB>* schedule_result);

  void ReleaseQuery(int64_t peak_mem_consumption);

  void ReleaseQueryBackends(const vector<NetworkAddressPB>& host_addr);

  void CancelAdmission();

 private:
  UniqueIdPB query_id_;

  /// True if this client should submit queries to the admission controller running
  /// in the local impalad.
  bool use_local_;

  /// If 'use_local_' is false, the address of the remote admission controller to use.
  TNetworkAddress address_;

  /// Promise used by the admission controller. AdmissionController:AdmitQuery() will
  /// block on this promise until the query is either rejected, admitted, times out, or is
  /// cancelled. Can be set to CANCELLED by the ClientRequestState in order to cancel, but
  /// otherwise is set by AdmissionController with the admission decision.
  Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome_;

  std::mutex lock_;

  /// If true, the AdmitQuery rpc has been sent but a final admission decision has not yet
  /// been recieved by GetQueryStatus().
  bool pending_admit_ = false;

  /// If 'use_local_' is true and admission has completed sucessfully, the resources
  /// allocated to this query, used to release them when calling ReleaseQuery() and
  /// ReleaseQueryBackends() on the local admission controller.
  std::unique_ptr<AdmissionController::QueryAllocation> allocation_;
};

} // namespace impala
