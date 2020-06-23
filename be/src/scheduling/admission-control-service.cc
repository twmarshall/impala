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

#include "scheduling/admission-control-service.h"

#include "common/constant-strings.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "runtime/mem-tracker.h"
#include "scheduling/ac-exec-env.h"
#include "scheduling/admission-controller.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"
#include "util/parse-util.h"
#include "util/uid-util.h"

#include "gen-cpp/admission_control_service.pb.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
    "AdmissionControlService. " + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(admission_control_service_queue_mem_limit, "50MB", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(admission_control_service_num_svc_threads, 0, "Number of threads for processing "
    "admission control service's RPCs. if left at default value 0, it will be set to number of "
    "CPU cores. Set it to a positive value to change from the default.");

namespace impala {

AdmissionControlService::AdmissionControlService()
  : AdmissionControlServiceIf(ACExecEnv::GetInstance()->rpc_mgr()->metric_entity(),
        ACExecEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = ACExecEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_admission_control_service_queue_mem_limit,
      &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid mem limit for admission control service queue: "
        "'$0'.", FLAGS_admission_control_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Adqmission Control Service Queue", process_mem_tracker));
}

Status AdmissionControlService::Init() {
  int num_svc_threads = FLAGS_admission_control_service_num_svc_threads > 0 ?
      FLAGS_admission_control_service_num_svc_threads : CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(ACExecEnv::GetInstance()->rpc_mgr()->RegisterService(num_svc_threads,
      std::numeric_limits<int32_t>::max(), this, mem_tracker_.get(), ACExecEnv::GetInstance()->rpc_metrics()));
  return Status::OK();
}

// Retrieves the sidecar at 'sidecar_idx' from 'rpc_context' and deserializes it into
// 'thrift_obj'.
template <typename T>
static Status GetSidecar(int sidecar_idx, RpcContext* rpc_context, T* thrift_obj) {
  kudu::Slice sidecar_slice;
  KUDU_RETURN_IF_ERROR(rpc_context->GetInboundSidecar(sidecar_idx, &sidecar_slice),
      "Failed to get sidecar");
  uint32_t len = sidecar_slice.size();
  RETURN_IF_ERROR(DeserializeThriftMsg(sidecar_slice.data(), &len, true, thrift_obj));
  return Status::OK();
}

void AdmissionControlService::AdmitQuery(const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp, RpcContext* rpc_context) {
  LOG(INFO) << "asdf AdmitQuery " << PrintId(req->query_id());
  TQueryExecRequest query_exec_request;
  const Status& sidecar_status1 =
      GetSidecar(req->query_exec_request_sidecar_idx(), rpc_context, &query_exec_request);
  if (!sidecar_status1.ok()) {
    LOG(INFO) << "asdf failed to deserialize TQueryExecRequest";
    RespondAndReleaseRpc(sidecar_status1, resp, rpc_context);
    return;
  }

  TQueryOptions query_options;
  const Status& sidecar_status2 =
      GetSidecar(req->query_options_sidecar_idx(), rpc_context, &query_options);
  if (!sidecar_status2.ok()) {
    LOG(INFO) << "asdf failed to deserialize TQueryOptions";
    RespondAndReleaseRpc(sidecar_status2, resp, rpc_context);
    return;
  }

  ObjectPool profile_pool;
  RuntimeProfile* summary_profile = RuntimeProfile::Create(&profile_pool, "Summary");
  RuntimeProfile::EventSequence* query_events = summary_profile->AddEventSequence("Query Timeline");
  query_events->Start();
  Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome;

  Status admit_status;
  std::unique_ptr<QuerySchedulePB> schedule;
  {
    //std::lock_guard<std::mutex> l(state->lock);
    admit_status =
      ACExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
          {req->query_id(), query_exec_request, query_options,
              summary_profile, query_events},
          &admit_outcome, &schedule);
    if (admit_status.ok()) {
      resp->mutable_query_schedule()->Swap(schedule.get());
      DCHECK(schedule.get() != nullptr);
    }
  }

  if (!admit_status.ok()) {
    LOG(INFO) << "asdf admit failed: " << admit_status;
    RespondAndReleaseRpc(admit_status, resp, rpc_context);
    return;
  }

  /*{
    std::lock_guard<std::mutex> l(queries_lock_);
    auto it = queries_.find(req->query_id());
    DCHECK(it == queries_.end());
    queries_.emplace(req->query_id(), make_unique<QueryAdmissionState>(schedule));
    }*/

  LOG(INFO) << "asdf AdmitQuery finished";

  RespondAndReleaseRpc(admit_status, resp, rpc_context);
}

void AdmissionControlService::ReleaseQuery(const ReleaseQueryRequestPB* req, ReleaseQueryResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  /*LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " done=" << (req->done() ? "true" : "false");

  vector<TNetworkAddress> host_addrs;
  for (const auto& entry : req->host_addr()) {
    host_addrs.push_back(FromNetworkAddressPB(entry));
  }
  LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " num hosts to release=" << host_addrs.size();

  QueryAdmissionState* state;
  {
    std::lock_guard<std::mutex> l(queries_lock_);
    auto it = queries_.find(req->query_id());
    DCHECK(it != queries_.end());

    state = queries_[req->query_id()].get();
    DCHECK(state != nullptr);
  }

  int pending;
  {
    std::lock_guard<std::mutex> l(state->lock);
    DCHECK(state->schedule != nullptr);
    LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " releasing backends pool=" << state->schedule->request_pool();
    if (host_addrs.size() > 0) {
      ACExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(*state->schedule.get(), host_addrs);
      for (int i = 0; i < host_addrs.size(); ++i) {
        state->backend_released_barrier.Notify();
      }
    }
    pending = state->backend_released_barrier.pending();
  }
  LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " done releasing backends";

  if (req->done()) {
    if (pending > 0) {
      LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " waiting on barrier";
      state->backend_released_barrier.Wait();
      LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " done waiting on barrier";
    }

    {
      std::lock_guard<std::mutex> l(state->lock);
      LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " releasing query";
      ACExecEnv::GetInstance()->admission_controller()->ReleaseQuery(*state->schedule.get(), req->peak_mem_consumption());
      LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " releasing query done";
    }

    std::lock_guard<std::mutex> l(queries_lock_);
    queries_.erase(req->query_id());
  }

  LOG(INFO) << "asdf ReleaseQuery " << PrintId(req->query_id()) << " done";*/

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

template <typename ResponsePBType>
void AdmissionControlService::RespondAndReleaseRpc(
    const Status& status, ResponsePBType* response, RpcContext* rpc_context) {
  status.ToProto(response->mutable_status());
  // Release the memory against the control service's memory tracker.
  mem_tracker_->Release(rpc_context->GetTransferSize());
  rpc_context->RespondSuccess();
}

} // namespace impala
