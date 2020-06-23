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

#include "common/status.h"

namespace impala {

class AdmissionController;
class AdmissionControlService;
class ClusterMembershipMgr;
class ImpalaHttpHandler;
class MemTracker;
class MetricGroup;
class PoolMemTrackerRegistry;
class RequestPoolService;
class RpcMgr;
class Scheduler;
class StatestoreSubscriber;
class Webserver;

class ACExecEnv {
 public:
  static ACExecEnv* GetInstance() { return ac_exec_env_; }

  ACExecEnv();
  ~ACExecEnv();

  Status Init();

  /// Starts the service to subscribe to the statestore.
  Status StartStatestoreSubscriberService() WARN_UNUSED_RESULT;

  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  MetricGroup* rpc_metrics() { return rpc_metrics_; }
  RpcMgr* rpc_mgr() const { return rpc_mgr_.get(); }

  ClusterMembershipMgr* cluster_membership_mgr() { return cluster_membership_mgr_.get(); }
  Scheduler* scheduler() { return scheduler_.get(); }
  AdmissionController* admission_controller() { return admission_controller_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }
  PoolMemTrackerRegistry* pool_mem_trackers() { return pool_mem_trackers_.get(); }

 private:
  static ACExecEnv* ac_exec_env_;

  std::unique_ptr<MetricGroup> metrics_;
  MetricGroup* rpc_metrics_ = nullptr;

  TNetworkAddress krpc_address_;
  std::unique_ptr<MemTracker> mem_tracker_;
  std::unique_ptr<RpcMgr> rpc_mgr_;
  std::unique_ptr<AdmissionControlService> admission_control_svc_;
  std::unique_ptr<ImpalaHttpHandler> http_handler_;
  std::unique_ptr<Webserver> webserver_;
  std::unique_ptr<PoolMemTrackerRegistry> pool_mem_trackers_;

  std::unique_ptr<RequestPoolService> request_pool_service_;

  std::unique_ptr<ClusterMembershipMgr> cluster_membership_mgr_;
  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<AdmissionController> admission_controller_;
  std::unique_ptr<StatestoreSubscriber> statestore_subscriber_;
};

} // namespace impala
