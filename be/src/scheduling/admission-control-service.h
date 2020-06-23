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

#include "gen-cpp/admission_control_service.service.h"

#include "common/status.h"
#include "util/container-util.h"
#include "util/counting-barrier.h"
#include "util/unique-id-hash.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class MemTracker;

class AdmissionControlService : public AdmissionControlServiceIf {
 public:
  AdmissionControlService();

  Status Init();

  virtual void AdmitQuery(const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp, kudu::rpc::RpcContext* rpc_context) override;

  virtual void ReleaseQuery(const ReleaseQueryRequestPB* req, ReleaseQueryResponsePB* resp, kudu::rpc::RpcContext* rpc_context) override;
 private:
  /*struct QueryAdmissionState {
   public:
    QueryAdmissionState(std::unique_ptr<QuerySchedule>& s) : schedule(std::move(s)), backend_released_barrier(schedule->per_backend_exec_params().size()) {}

    std::unique_ptr<QuerySchedule> schedule;
    std::mutex lock;
    /// Barrier that is released when all Backends have released their admission control
    /// resources.
    CountingBarrier backend_released_barrier;
  };

  std::mutex queries_lock_;

  std::unordered_map<UniqueIdPB, std::unique_ptr<QueryAdmissionState>> queries_;*/

  /// Tracks the memory usage of payload in the service queue.
  std::unique_ptr<MemTracker> mem_tracker_;

  /// Helper for serializing 'status' as part of 'response'. Also releases memory
  /// of the RPC payload previously accounted towards the internal memory tracker.
  template <typename ResponsePBType>
  void RespondAndReleaseRpc(
      const Status& status, ResponsePBType* response, kudu::rpc::RpcContext* rpc_context);
};

} // namespace impala
