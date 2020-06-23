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

//
// This file contains the main() function for the admission controller process,
// which exports the KRPC service AdmissionControlService.

#include "common/init.h"
#include "common/logging.h"
#include "scheduling/ac-exec-env.h"

DECLARE_int32(krpc_port);

using namespace impala;

int AdmissionControldMain(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);
  LOG(INFO) << "asdf AdmissionControldMain";

  ACExecEnv ac_exec_env;
  LOG(INFO) << "asdf AdmissionControldMain 1";
  ABORT_IF_ERROR(ac_exec_env.Init());
  LOG(INFO) << "asdf AdmissionControldMain 2";
  ABORT_IF_ERROR(ac_exec_env.StartStatestoreSubscriberService());
  LOG(INFO) << "asdf AdmissionControldMain 3";
  while(true) {}

  return 0;
}
