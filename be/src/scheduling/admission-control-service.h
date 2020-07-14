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

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/admission_control_service.proxy.h"
#include "gen-cpp/admission_control_service.service.h"
#include "scheduling/admission-controller.h"
#include "util/sharded-query-map-util.h"
#include "util/thread-pool.h"
#include "util/unique-id-hash.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class MemTracker;
class MetricGroup;
class QuerySchedulePB;

/// Singleton class that exports the RPC service used for submitting queries remotely for
/// admission.
class AdmissionControlService : public AdmissionControlServiceIf {
 public:
  AdmissionControlService(MetricGroup* metric_group);

  /// Initializes the service by registering it with the singleton RPC manager.
  /// This mustn't be called until RPC manager has been initialized.
  Status Init();

  virtual void AdmitQuery(const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp,
      kudu::rpc::RpcContext* context) override;
  virtual void GetQueryStatus(const GetQueryStatusRequestPB* req,
      GetQueryStatusResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void ReleaseQuery(const ReleaseQueryRequestPB* req,
      ReleaseQueryResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void ReleaseQueryBackends(const ReleaseQueryBackendsRequestPB* req,
      ReleaseQueryBackendsResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void CancelAdmission(const CancelAdmissionRequestPB* req,
      CancelAdmissionResponsePB* resp, kudu::rpc::RpcContext* context) override;

  /// Gets a AdmissionControlService proxy to a server with 'address' and 'hostname'.
  /// The newly created proxy is returned in 'proxy'. Returns error status on failure.
  static Status GetProxy(const TNetworkAddress& address, const std::string& hostname,
      std::unique_ptr<AdmissionControlServiceProxy>* proxy);

 private:
  friend class ImpalaHttpHandler;

  /// Tracks the memory usage of payload in the service queue.
  std::unique_ptr<MemTracker> mem_tracker_;

  struct QueryInfo {
   public:
    QueryInfo(const UniqueIdPB& query_id, const UniqueIdPB& coord_id)
      : query_id(query_id), coord_id(coord_id) {}

    UniqueIdPB query_id;

    UniqueIdPB coord_id;

    TQueryExecRequest query_exec_request;

    TQueryOptions query_options;

    // Protects all of the following members.
    std::mutex lock;

    bool submitted = false;

    bool admission_done = false;

    Status admit_status;

    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome;

    std::unique_ptr<QuerySchedulePB> schedule;

    std::unique_ptr<AdmissionController::QueryAllocation> allocation;

    ObjectPool profile_pool;

    RuntimeProfile* summary_profile;
    RuntimeProfile::EventSequence* query_events;
  };

  /// Used to perform the actual work of scheduling and admitting queries, so that
  /// AdmitQuery() can return immediately.
  std::unique_ptr<ThreadPool<UniqueIdPB>> admission_thread_pool_;

  /// Thread-safe map from query ids fo info about the query.
  ShardedQueryIdPBMap<std::shared_ptr<QueryInfo>> query_info_map_;

  /// Callback for 'admission_thread_pool_'.
  void AdmitFromThreadPool(UniqueIdPB query_id);

  /// Helper for serializing 'status' as part of 'response'. Also releases memory
  /// of the RPC payload previously accounted towards the internal memory tracker.
  template <typename ResponsePBType>
  void RespondAndReleaseRpc(
      const Status& status, ResponsePBType* response, kudu::rpc::RpcContext* rpc_context);
};

} // namespace impala
