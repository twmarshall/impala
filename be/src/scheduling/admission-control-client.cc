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

#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/admission_control_service.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "rpc/sidecar-util.h"
#include "runtime/exec-env.h"
#include "scheduling/admission-control-service.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "common/names.h"

DECLARE_string(admission_control_service_addr);
DECLARE_bool(is_admission_controller);

using namespace strings;
using namespace kudu::rpc;

namespace impala {

const string QUERY_EVENT_COMPLETED_ADMISSION = "Completed admission";

AdmissionControlClient::AdmissionControlClient(const TUniqueId& query_id)
  : use_local_(
        FLAGS_is_admission_controller || FLAGS_admission_control_service_addr.empty()),
    address_(MakeNetworkAddress(FLAGS_admission_control_service_addr)) {
  TUniqueIdToUniqueIdPB(query_id, &query_id_);
}

Status AdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    std::unique_ptr<QuerySchedulePB>* schedule_result) {
  // We track this outside of the queue node so that it is still available after the query
  // has been dequeued.
  ScopedEvent completedEvent(request.query_events, QUERY_EVENT_COMPLETED_ADMISSION);

  if (use_local_) {
    bool queued;
    Status status = ExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
        request, &admit_outcome_, schedule_result, &allocation_, &queued);
    if (queued) {
      DCHECK(status.ok());
      status = ExecEnv::GetInstance()->admission_controller()->WaitOnQueued(
          request.query_id, schedule_result, &allocation_);
    }
    return status;
  }
  request.query_events->MarkEvent("Sending admission request");
  LOG(INFO) << "asdf Submitting remotely";
  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
  DCHECK(get_proxy_status.ok()) << get_proxy_status;
  AdmitQueryRequestPB req;
  AdmitQueryResponsePB resp;
  RpcController rpc_controller;

  *req.mutable_query_id() = request.query_id;
  *req.mutable_coord_id() = ExecEnv::GetInstance()->backend_id();

  KrpcSerializer serializer;
  int sidecar_idx1;
  RETURN_IF_ERROR(
      serializer.SerializeToSidecar(&request.request, &rpc_controller, &sidecar_idx1));
  req.set_query_exec_request_sidecar_idx(sidecar_idx1);

  KrpcSerializer serializer2;
  int sidecar_idx2;
  RETURN_IF_ERROR(serializer2.SerializeToSidecar(
      &request.query_options, &rpc_controller, &sidecar_idx2));
  req.set_query_options_sidecar_idx(sidecar_idx2);

  {
    lock_guard<mutex> l(lock_);
    if (admit_outcome_.IsSet()) {
      DCHECK(admit_outcome_.Get() == AdmissionOutcome::CANCELLED);
      return Status("Query already cancelled.");
    }

    kudu::Status admit_rpc_status = proxy->AdmitQuery(req, &resp, &rpc_controller);
    LOG(INFO) << "asdf rpc status=" << FromKuduStatus(admit_rpc_status, "asdf");
    LOG(INFO) << "asdf resp status=" << Status(resp.status());
    DCHECK(admit_rpc_status.ok()); // TODO
    DCHECK(Status(resp.status()).ok()) << Status(resp.status());

    pending_admit_ = true;
  }

  Status admit_status = Status::OK();
  while (true) {
    sleep(.1);

    RpcController rpc_controller2;
    GetQueryStatusRequestPB get_status_req;
    GetQueryStatusResponsePB get_status_resp;
    *get_status_req.mutable_query_id() = request.query_id;
    kudu::Status get_status_status =
        proxy->GetQueryStatus(get_status_req, &get_status_resp, &rpc_controller2);
    DCHECK(get_status_status.ok()) << FromKuduStatus(get_status_status, "asdf");

    if (get_status_resp.has_summary_profile_sidecar_idx()) {
      TRuntimeProfileTree tree;
      RETURN_IF_ERROR(GetSidecar(
          get_status_resp.summary_profile_sidecar_idx(), &rpc_controller2, &tree));
      request.summary_profile->Update(tree);
    }

    if (get_status_resp.has_query_schedule()) {
      schedule_result->reset(new QuerySchedulePB());
      schedule_result->get()->Swap(get_status_resp.mutable_query_schedule());
      break;
    }
    admit_status = Status(get_status_resp.status());
    if (!admit_status.ok()) {
      break;
    }
  }

  {
    lock_guard<mutex> l(lock_);
    pending_admit_ = false;
  }

  request.query_events->MarkEvent("Received admission response");
  return admit_status;
}

void AdmissionControlClient::ReleaseQuery(int64_t peak_mem_consumption) {
  if (use_local_) {
    ExecEnv::GetInstance()->admission_controller()->ReleaseQuery(
        query_id_, peak_mem_consumption, *allocation_.get());
    return;
  }
  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
  DCHECK(get_proxy_status.ok()) << get_proxy_status;

  ReleaseQueryRequestPB req;
  ReleaseQueryResponsePB resp;
  *req.mutable_query_id() = query_id_;
  req.set_peak_mem_consumption(peak_mem_consumption);
  RpcController rpc_controller;
  kudu::Status rpc_status = proxy->ReleaseQuery(req, &resp, &rpc_controller);

  // Failure of this rpc is not considered a query failure, so we just log it.
  // TODO: we need to be sure that the resources to in fact get cleaned up in situation
  // like these (IMPALA-9976).
  if (!rpc_status.ok()) {
    LOG(WARNING) << "ReleaseQuery rpc failed for " << query_id_ << ": "
                 << rpc_status.ToString();
  }
  Status resp_status(resp.status());
  if (!resp_status.ok()) {
    LOG(WARNING) << "ReleaseQuery failed for " << query_id_ << ": " << resp_status;
  }
}

void AdmissionControlClient::ReleaseQueryBackends(
    const vector<NetworkAddressPB>& host_addrs) {
  if (use_local_) {
    ExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
        query_id_, host_addrs, *allocation_.get());
    return;
  }
  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
  DCHECK(get_proxy_status.ok()) << get_proxy_status;

  ReleaseQueryBackendsRequestPB req;
  ReleaseQueryBackendsResponsePB resp;
  *req.mutable_query_id() = query_id_;
  for (const NetworkAddressPB& addr : host_addrs) {
    *req.add_host_addr() = addr;
  }
  RpcController rpc_controller;
  kudu::Status rpc_status = proxy->ReleaseQueryBackends(req, &resp, &rpc_controller);

  // Failure of this rpc is not considered a query failure, so we just log it.
  // TODO: we need to be sure that the resources to in fact get cleaned up in situation
  // like these (IMPALA-9976).
  if (!rpc_status.ok()) {
    LOG(WARNING) << "ReleaseQueryBackends rpc failed for " << query_id_ << ": "
                 << rpc_status.ToString();
  }
  Status resp_status(resp.status());
  if (!resp_status.ok()) {
    LOG(WARNING) << "ReleaseQueryBackends failed for " << query_id_ << ": " << resp_status;
  }
}

void AdmissionControlClient::CancelAdmission() {
  LOG(INFO) << "asdf CancelAdmission";
  bool needs_cancel;
  {
    lock_guard<mutex> l(lock_);
    admit_outcome_.Set(AdmissionOutcome::CANCELLED);
    if (use_local_) return;
    needs_cancel = pending_admit_;
  }

  if (needs_cancel) {
    LOG(INFO) << "asdf CancelAdmission sending rpc";
    std::unique_ptr<AdmissionControlServiceProxy> proxy;
    Status get_proxy_status =
        AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
    DCHECK(get_proxy_status.ok()) << get_proxy_status;

    CancelAdmissionRequestPB req;
    CancelAdmissionResponsePB resp;
    *req.mutable_query_id() = query_id_;
    RpcController rpc_controller;
    kudu::Status rpc_status = proxy->CancelAdmission(req, &resp, &rpc_controller);

    // Failure of this rpc is not considered a query failure, so we just log it.
    if (!rpc_status.ok()) {
      LOG(WARNING) << "CancelAdmission rpc failed for " << query_id_ << ": "
                   << rpc_status.ToString();
    }
    Status resp_status(resp.status());
    if (!resp_status.ok()) {
      LOG(WARNING) << "CancelAdmission failed for " << query_id_ << ": " << resp_status;
    }
  }
}

} // namespace impala
