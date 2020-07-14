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
#include "gen-cpp/admission_control_service.pb.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/sidecar-util.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/admission-controller.h"
#include "util/cpu-info.h"
#include "util/kudu-status-util.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "util/promise.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
                                      "AdmissionControlService. "
    + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(admission_control_service_queue_mem_limit, "50MB", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(admission_control_service_num_svc_threads, 0,
    "Number of threads for processing "
    "admission control service's RPCs. if left at default value 0, it will be set to "
    "number of "
    "CPU cores. Set it to a positive value to change from the default.");
DEFINE_int32(admission_thread_pool_size, 50,
    "(Advanced) Size of the thread-pool processing AdmitQuery requests.");
DEFINE_int32(max_admission_queue_size, 5,
    "(Advanced) Max size of the queue for the AdmitQuery thread pool.");

DEFINE_string(admission_control_service_addr, "",
    "(Experimental) If "
    "provided, queries submitted to this impalad will be scheduled and admitted by "
    "contacting the admission control service at the specified address. This flag will "
    "be removed in a future version, see IMPALA-9155.");

namespace impala {

#define RESPOND_IF_ERROR(stmt)                          \
  do {                                                  \
    const Status& _status = (stmt);                     \
    if (UNLIKELY(!_status.ok())) {                      \
      RespondAndReleaseRpc(_status, resp, rpc_context); \
      return;                                           \
    }                                                   \
  } while (false)

AdmissionControlService::AdmissionControlService(MetricGroup* metric_group)
  : AdmissionControlServiceIf(ExecEnv::GetInstance()->rpc_mgr()->metric_entity(),
        ExecEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit =
      ParseUtil::ParseMemSpec(FLAGS_admission_control_service_queue_mem_limit,
          &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid mem limit for admission control service queue: "
                   "'$0'.",
            FLAGS_admission_control_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Adqmission Control Service Queue", process_mem_tracker));
  // MemTrackerMetric::CreateMetrics(metric_group, mem_tracker_.get(), "ControlService");
}

Status AdmissionControlService::Init() {
  int num_svc_threads = FLAGS_admission_control_service_num_svc_threads > 0 ?
      FLAGS_admission_control_service_num_svc_threads :
      CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->RegisterService(
      num_svc_threads, std::numeric_limits<int32_t>::max(), this, mem_tracker_.get()));

  admission_thread_pool_.reset(
      new ThreadPool<UniqueIdPB>("admission-control-service", "admission-worker",
          FLAGS_admission_thread_pool_size, FLAGS_max_admission_queue_size,
          bind<void>(&AdmissionControlService::AdmitFromThreadPool, this, _2)));
  ABORT_IF_ERROR(admission_thread_pool_->Init());

  return Status::OK();
}

Status AdmissionControlService::GetProxy(const TNetworkAddress& address,
    const string& hostname, unique_ptr<AdmissionControlServiceProxy>* proxy) {
  // Create a ControlService proxy to the destination.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(address, hostname, proxy));
  // Use a network plane different from DataStreamService's to avoid being blocked by
  // large payloads in DataStreamService.
  (*proxy)->set_network_plane("control");
  return Status::OK();
}

void AdmissionControlService::AdmitQuery(
    const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp, RpcContext* rpc_context) {
  VLOG(1) << "AdmitQuery: query_id=" << req->query_id()
          << " coordinator=" << req->coord_id();

  shared_ptr<QueryInfo> query_info;
  query_info = make_shared<QueryInfo>(req->query_id(), req->coord_id());
  RESPOND_IF_ERROR(query_info_map_.Add(req->query_id(), query_info));

  {
    lock_guard<mutex> l(query_info->lock);

    query_info->summary_profile =
        RuntimeProfile::Create(&query_info->profile_pool, "Summary");
    query_info->query_events =
        query_info->summary_profile->AddEventSequence("Admission Control Timeline");
    query_info->query_events->Start();
    query_info->query_events->MarkEvent("Request received");

    RESPOND_IF_ERROR(GetSidecar(req->query_exec_request_sidecar_idx(), rpc_context,
        &query_info->query_exec_request));

    RESPOND_IF_ERROR(GetSidecar(
        req->query_options_sidecar_idx(), rpc_context, &query_info->query_options));

    admission_thread_pool_->Offer(req->query_id());
  }

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::GetQueryStatus(const GetQueryStatusRequestPB* req,
    GetQueryStatusResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  LOG(INFO) << "asdf GetQueryStatus " << req->query_id();

  shared_ptr<QueryInfo> query_info;
  RESPOND_IF_ERROR(query_info_map_.Get(req->query_id(), &query_info));

  Status status = Status::OK();
  {
    lock_guard<mutex> l(query_info->lock);
    if (query_info->submitted) {
      if (!query_info->admission_done) {
        bool timed_out;
        query_info->admit_status =
            ExecEnv::GetInstance()->admission_controller()->WaitOnQueued(req->query_id(),
                &query_info->schedule, &query_info->allocation, 100, &timed_out);
        if (!timed_out) {
          query_info->admission_done = true;
        } else {
          DCHECK(query_info->admit_status.ok());
        }
      }

      if (query_info->admission_done) {
        query_info->query_events->MarkEvent("Returned schedule");

        if (query_info->admit_status.ok()) {
          *resp->mutable_query_schedule() = *query_info->schedule.get();
        } else {
          status = query_info->admit_status;
        }
      }

      // Always send the profile even if admission isn't done yet.
      TRuntimeProfileTree tree;
      query_info->summary_profile->ToThrift(&tree);
      int sidecar_idx;

      RESPOND_IF_ERROR(SetFaststringSidecar(tree, rpc_context, &sidecar_idx));
      resp->set_summary_profile_sidecar_idx(sidecar_idx);
    }
  }

  LOG(INFO) << "asdf GetQueryStatus done " << query_info->query_id << " " << status;
  RespondAndReleaseRpc(status, resp, rpc_context);
}

void AdmissionControlService::ReleaseQuery(const ReleaseQueryRequestPB* req,
    ReleaseQueryResponsePB* resp, RpcContext* rpc_context) {
  VLOG(1) << "ReleaseQuery: query_id=" << req->query_id();
  shared_ptr<QueryInfo> query_info;
  RESPOND_IF_ERROR(query_info_map_.Get(req->query_id(), &query_info));

  {
    lock_guard<mutex> l(query_info->lock);
    ExecEnv::GetInstance()->admission_controller()->ReleaseQuery(
        req->query_id(), req->peak_mem_consumption(), *query_info->allocation.get());
  }

  RESPOND_IF_ERROR(query_info_map_.Delete(req->query_id()));
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::ReleaseQueryBackends(
    const ReleaseQueryBackendsRequestPB* req, ReleaseQueryBackendsResponsePB* resp,
    RpcContext* rpc_context) {
  VLOG(1) << "ReleaseQueryBackends: query_id=" << req->query_id();
  shared_ptr<QueryInfo> query_info;
  RESPOND_IF_ERROR(query_info_map_.Get(req->query_id(), &query_info));

  vector<NetworkAddressPB> host_addrs;
  for (const NetworkAddressPB host_addr : req->host_addr()) {
    host_addrs.push_back(host_addr);
  }

  {
    lock_guard<mutex> l(query_info->lock);
    ExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
        req->query_id(), host_addrs, *query_info->allocation.get());
  }

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::CancelAdmission(const CancelAdmissionRequestPB* req,
    CancelAdmissionResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  VLOG(1) << "CancelAdmission: query_id=" << req->query_id();
  shared_ptr<QueryInfo> query_info;
  RESPOND_IF_ERROR(query_info_map_.Get(req->query_id(), &query_info));

  // We do not take QueryInfo::lock_ here because 'admit_outcome' is thread-safe and we
  // want to be able to cancel the admisison concurrently with other calls like
  // SubmitForAdmission()
  query_info->admit_outcome.Set(AdmissionOutcome::CANCELLED);

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::AdmitFromThreadPool(UniqueIdPB query_id) {
  shared_ptr<QueryInfo> query_info;
  Status s = query_info_map_.Get(query_id, &query_info);
  if (!s.ok()) {
    LOG(ERROR) << s;
    return;
  }

  {
    lock_guard<mutex> l(query_info->lock);
    bool queued;
    query_info->admit_status =
        ExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
            {query_info->query_id, query_info->coord_id, query_info->query_exec_request,
                query_info->query_options, query_info->summary_profile,
                query_info->query_events},
            &query_info->admit_outcome, &query_info->schedule, &query_info->allocation,
            &queued);
    query_info->submitted = true;
    if (!queued) {
      query_info->admission_done = true;
    }
  }
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
