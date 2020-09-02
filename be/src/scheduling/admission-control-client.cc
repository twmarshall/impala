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

#include "scheduling/admission-control-client.h"

#include "runtime/exec-env.h"
#include "util/uid-util.h"

#include "common/names.h"

namespace impala {

AdmissionControlClient::AdmissionControlClient(const TUniqueId& query_id)
  : use_local_(true) {
  TUniqueIdToUniqueIdPB(query_id, &query_id_);
}

Status AdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    std::unique_ptr<QuerySchedulePB>* schedule_result) {
  if (use_local_) {
    Status status = ExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
        request, &admit_outcome_, schedule_result);
    return status;
  }

  DCHECK(false) << "Not implemented.";
  return Status::OK();
}

void AdmissionControlClient::ReleaseQuery(int64_t peak_mem_consumption) {
  if (use_local_) {
    ExecEnv::GetInstance()->admission_controller()->ReleaseQuery(
        query_id_, peak_mem_consumption);
    return;
  }

  DCHECK(false) << "Not implemented.";
}

void AdmissionControlClient::ReleaseQueryBackends(
    const vector<NetworkAddressPB>& host_addrs) {
  if (use_local_) {
    ExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
        query_id_, host_addrs);
    return;
  }

  DCHECK(false) << "Not implemented.";
}

void AdmissionControlClient::CancelAdmission() {
  if (use_local_) {
    admit_outcome_.Set(AdmissionOutcome::CANCELLED);
    return;
  }
  DCHECK(false) << "Not implemented";
}

} // namespace impala
