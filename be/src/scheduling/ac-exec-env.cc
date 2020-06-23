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

#include "scheduling/ac-exec-env.h"

#include "rpc/rpc-mgr.h"
#include "runtime/mem-tracker.h"
#include "service/impala-http-handler.h"
#include "scheduling/admission-control-service.h"
#include "scheduling/admission-controller.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/scheduler.h"
#include "statestore/statestore-subscriber.h"
#include "util/metrics.h"

#include "common/names.h"

DECLARE_string(hostname);
DECLARE_string(state_store_host);
DECLARE_int32(state_store_port);
DECLARE_int32(state_store_subscriber_port);

namespace impala {

ACExecEnv* ACExecEnv::ac_exec_env_ = nullptr;

ACExecEnv::ACExecEnv() :
    metrics_(new MetricGroup("ac-impala-metrics")),
    rpc_metrics_(metrics_->GetOrCreateChildGroup("rpc")),
    webserver_(new Webserver("127.0.0.1", 29550, metrics_.get())),
    pool_mem_trackers_(new PoolMemTrackerRegistry) {
  LOG(INFO) << "asdf new ACExecEnv";
  TNetworkAddress configured_address = MakeNetworkAddress("127.0.0.1", 29500);
  krpc_address_.__set_hostname("127.0.0.1");
  krpc_address_.__set_port(29500);
  rpc_mgr_.reset(new RpcMgr(IsInternalTlsConfigured()));

  request_pool_service_.reset(new RequestPoolService(metrics_.get()));

  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, 23005); //FLAGS_state_store_subscriber_port); TODO
  TNetworkAddress statestore_address =
      MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);

  statestore_subscriber_.reset(new StatestoreSubscriber(
      Substitute("ac@$0", TNetworkAddressToString(configured_address)),
      subscriber_address, statestore_address, metrics_.get()));

  scheduler_.reset(new Scheduler(metrics_.get(), request_pool_service_.get()));

  cluster_membership_mgr_.reset(new ClusterMembershipMgr(
      statestore_subscriber_->id(), statestore_subscriber_.get(), metrics_.get()));

  admission_controller_.reset(
      new AdmissionController(cluster_membership_mgr_.get(), statestore_subscriber_.get(),
          request_pool_service_.get(), metrics_.get(), configured_address));

  ac_exec_env_ = this;
}

ACExecEnv::~ACExecEnv() {
  if (rpc_mgr_ != nullptr) rpc_mgr_->Shutdown();
}

Status ACExecEnv::Init() {
  RETURN_IF_ERROR(metrics_->Init(webserver_.get()));

  mem_tracker_.reset(new MemTracker());

  RETURN_IF_ERROR(rpc_mgr_->Init(krpc_address_));
  admission_control_svc_.reset(new AdmissionControlService());
  RETURN_IF_ERROR(admission_control_svc_->Init());
  RETURN_IF_ERROR(rpc_mgr_->StartServices());

  //http_handler_.reset(new ImpalaHttpHandler(nullptr));
  //http_handler_->RegisterAdmissionHandlers(webserver_.get());
  RETURN_IF_ERROR(webserver_->Start());

  RETURN_IF_ERROR(cluster_membership_mgr_->Init());

  RETURN_IF_ERROR(admission_controller_->Init());

  return Status::OK();
}

Status ACExecEnv::StartStatestoreSubscriberService() {
  LOG(INFO) << "Starting statestore subscriber service";

  // Must happen after all topic registrations / callbacks are done
  if (statestore_subscriber_.get() != nullptr) {
    Status status = statestore_subscriber_->Start();
    if (!status.ok()) {
      status.AddDetail("Statestore subscriber did not start up.");
      return status;
    }
  }

  return Status::OK();
}

} // namespace impala
