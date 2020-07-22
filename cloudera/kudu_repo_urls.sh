#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The text-replace directives do not preserve the executable bit, so
# this file is sourced from cdp_install_cmd.sh rather than embedding the
# variables themselves.

# These values for KUDU_REPO_URL/KUDU_GBN_URL are replaced during the build:
export KUDU_REPO_URL=http://nexus-private.hortonworks.com/nexus/content/groups/public
export KUDU_GBN_URL=http://nexus-private.hortonworks.com/nexus/content/groups/public
