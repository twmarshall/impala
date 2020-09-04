# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Variables in the file override the default values from impala-config.sh.
# Config changes for release or features branches should go here so they
# can be version controlled but not conflict with changes on the master
# branch.

# Use CDPD Hive
export USE_CDP_HIVE=true

# How Impala gets its versions depends on how it is building. If someone checks out
# an Impala branch and builds, this is a "standalone" build and versions come from a
# CDP GBN. We assume this is a standalone build unless the IS_STANDALONE_IMPALA_BUILD
# environment variable is set to false.
IS_STANDALONE_IMPALA_BUILD=${IS_STANDALONE_IMPALA_BUILD-true}

# The downstream CDP build is not a standalone build. Downstream CDP builds need to be
# able to build the entirety of the CDP distribution from scratch. This means that it
# cannot rely on a fully-formed GBN to build against, because it is creating that GBN.
# Instead, the build system does text replacements to fill in the versions that it
# is using for this build. The below section is where the variables are filled in for
# the CDP build.
#
# Note: The set_snapshot_versions Jenkins job also uses text replacements, so the
# following versions will be updated with SNAPSHOT versions. However, Impala does not
# use these SNAPSHOT versions. Instead, Impala gets version from a CDP GBN.
if ! ${IS_STANDALONE_IMPALA_BUILD}; then
    export CDP_HADOOP_VERSION=3.1.1.7.2.2.0-SNAPSHOT
    export CDP_HBASE_VERSION=2.2.5.7.2.2.0-SNAPSHOT
    export CDP_HIVE_VERSION=3.1.3000.7.2.2.0-SNAPSHOT
    export CDP_KNOX_VERSION=1.3.0.7.2.2.0-SNAPSHOT
    export CDP_KUDU_VERSION=1.12.0.7.2.2.0-SNAPSHOT
    export CDP_KUDU_JAVA_VERSION=1.12.0.7.2.2.0-SNAPSHOT
    export CDP_RANGER_VERSION=2.0.0.7.2.2.0-SNAPSHOT
    export CDP_TEZ_VERSION=0.9.1.7.2.2.0-SNAPSHOT
    export CDP_OZONE_VERSION=0.6.0.7.2.2.0-SNAPSHOT
    export CDP_ATLAS_VERSION=2.0.0.7.2.2.0-SNAPSHOT
    export CDP_AVRO_JAVA_VERSION=1.8.2.7.2.2.0-SNAPSHOT
    export CDP_PARQUET_VERSION=1.10.99.7.2.2.0-SNAPSHOT
    # TODO: remove these IMPALA_KUDU_* once there is proper support of CDP Kudu
    export IMPALA_KUDU_VERSION=1.12.0.7.2.2.0-SNAPSHOT
    export IMPALA_KUDU_JAVA_VERSION=1.12.0.7.2.2.0-SNAPSHOT
    return 0
fi;

# Standalone Impala builds use $CDP_GBN to specify which build we are working against.
# This is determined as follows.
# 1. If $GLOBAL_BUILD_NUMBER is set, use that for $CDP_GBN.
# 2. If $CDP_GBN is set, use that.
# 3. If $IMPALA_HOME/toolchain/cdp_components/cdp-gbn-${CDP_VERSION}.sh exists, source it
#    to set $CDP_GBN. This will cause subsequent builds in the same $IMPALA_HOME
#    to continue using the same cdp components.
# 4. Query BuildDB for the latest official CDP build for ${CDP_VERSION}, and use that.
#    The result is cached in cdp-gbn-${CDP_VERSION}.sh so that a workspace
#    continues to use the same components.
# In addition to being used by bin/bootstrap-toolchain.sh to download the
# tarballs to support the minicluster, CDP_GBN is used to set CDP_MAVEN_REPOSITORY
# to the maven repository from that GBN (used in impala-parent/pom.xml).
#
# Furthermore, a cdh-root.properties [SIC] file matching the GBN is used
# to figure out the CDP component versions.

# Defer to $GLOBAL_BUILD_NUMBER OR $CDP_GBN, in that order.
export CDP_GBN=${GLOBAL_BUILD_NUMBER:-${CDP_GBN:-}}

# The CDP_VERSION is used to query builddb. This will be different on different branches.
# It is set by the set_snapshot_versions Jenkins job.
export CDP_VERSION=7.2.3.0
# The file name to stash the GBN incorporates $CDP_VERSION to make distinctions between
# releases.
CDP_GBN_CONFIG="${IMPALA_HOME}/toolchain/cdp_components/cdp-gbn-${CDP_VERSION}.sh"
WGET="wget --no-verbose -O -"

if [ ! "${CDP_GBN}" ]; then
  if [ -f "${CDP_GBN_CONFIG}" ]; then
    . "${CDP_GBN_CONFIG}"
    echo "Using CDP_GBN ${CDP_GBN} based on ${CDP_GBN_CONFIG}"
  else
    # Require an official build that has a redhat7 build
    BUILDDB_QUERY='http://builddb.infra.cloudera.com/query?product=cdh;tag=official,redhat7_repo;version='"${CDP_VERSION}"
    export CDP_GBN=$($WGET "${BUILDDB_QUERY}")
    [ "${CDP_GBN}" ]  # Assert we got something
    mkdir -p "$(dirname ${CDP_GBN_CONFIG})"
    echo "export CDP_GBN=${CDP_GBN}" > "${CDP_GBN_CONFIG}"
    echo "Using CDP_GBN ${CDP_GBN} based on BuildDB query ${BUILDDB_QUERY}."
  fi
fi
export CDP_BUILD_NUMBER=${CDP_GBN}

# Pick the build cache located in the AWS region where the builds happen.
# TODO: Build smarter logic that figures this out dynamically. Optionally
#       rely on the buildinfo tools.
BUILD_REPO="http://cloudera-build-4-us-west-2.vpc.cloudera.com/s3/build/${CDP_GBN}"
BUILD_REPO_BASE="${BUILD_REPO}/cdh/7.x/redhat7/yum/tars"

# Use the maven repository from this GBN
export CDP_MAVEN_REPOSITORY="${BUILD_REPO}/cdh/7.x/maven-repository/"

# The cdh-root.properties file contains versions for all components.
# Download it and store it in a known location (if it is not already downloaded).
CDP_PROPERTIES_FILE="${IMPALA_HOME}/toolchain/cdp_components/cdp.properties.${CDP_GBN}"
if [ ! -f "${CDP_PROPERTIES_FILE}" ]; then
    mkdir -p "$(dirname ${CDP_PROPERTIES_FILE})"
    $WGET "${BUILD_REPO}"/cdh-root.properties -O "${CDP_PROPERTIES_FILE}"
fi

# Get Java version strings from cdh-root.properties and determine URLs based on
# those version strings.
function get_cdp_version {
  local COMPONENT=$(echo ${1} | awk '{ print tolower($0) }')
  local UPPERCASE_COMPONENT=$(echo ${1} | awk '{ print toupper($0) }')
  local VERSION=$(cat ${CDP_PROPERTIES_FILE} | grep ^cdh.${COMPONENT}.version | cut -d= -f2)
  echo "Version for ${UPPERCASE_COMPONENT}: $VERSION"
  export CDP_${UPPERCASE_COMPONENT}_VERSION=${VERSION}
}

get_cdp_version hadoop
get_cdp_version hbase
get_cdp_version hive
get_cdp_version knox
get_cdp_version kudu
get_cdp_version ranger
get_cdp_version tez
get_cdp_version ozone
get_cdp_version atlas
get_cdp_version avro
get_cdp_version parquet

[[ -n $CDP_HADOOP_VERSION ]]
[[ -n $CDP_HBASE_VERSION ]]
[[ -n $CDP_HIVE_VERSION ]]
[[ -n $CDP_KNOX_VERSION ]]
[[ -n $CDP_KUDU_VERSION ]]
[[ -n $CDP_RANGER_VERSION ]]
[[ -n $CDP_TEZ_VERSION ]]
[[ -n $CDP_OZONE_VERSION ]]
[[ -n $CDP_ATLAS_VERSION ]]
[[ -n $CDP_AVRO_VERSION ]]
[[ -n $CDP_PARQUET_VERSION ]]

# Kudu Java version matches the Kudu version
# Ugly hack: We don't want this to be replaced. Break it into two statements so it
# doesn't match the "export CDP_KUDU_JAVA_VERSION" regex.
CDP_KUDU_JAVA_VERSION=${CDP_KUDU_VERSION}
export CDP_KUDU_JAVA_VERSION

# Avro Java version matches the Avro version
# Ugly hack: We don't want this to be replaced. Break it into two statements so it
# doesn't match the "export CDP_AVRO_JAVA_VERSION" regex.
CDP_AVRO_JAVA_VERSION=${CDP_AVRO_VERSION}
export CDP_AVRO_JAVA_VERSION

# Compute the URLs for the minicluster tarballs
# ${version} in CDP_<PACKAGE>_URL will be replaced by __compute_version() in
# bin/bootstrap_toolchain.py with the version retrieved from the environment variable
# IMPALA_<PACKAGE>_VERSION set up in bin/impala-config.sh.
export CDP_HADOOP_URL=${BUILD_REPO_BASE}/hadoop/hadoop-'${version}'.tar.gz
export CDP_HBASE_URL=${BUILD_REPO_BASE}/hbase/hbase-'${version}'-bin.tar.gz
export CDP_HIVE_URL=${BUILD_REPO_BASE}/hive/apache-hive-'${version}'-bin.tar.gz
export CDP_HIVE_SOURCE_URL=${BUILD_REPO_BASE}/hive/hive-'${version}'-source.tar.gz
export CDP_KUDU_URL=${BUILD_REPO_BASE}/impala/kudu-for-impala-'${version}'.tar.gz
export CDP_KUDU_JARS_URL=${BUILD_REPO_BASE}/impala/kudu-jars-'${version}'.tar.gz
export CDP_TEZ_URL=${BUILD_REPO_BASE}/tez/tez-'${version}'-minimal.tar.gz
export CDP_RANGER_URL=${BUILD_REPO_BASE}/ranger/ranger-'${version}'-admin.tar.gz
export CDP_ATLAS_URL=${BUILD_REPO_BASE}/atlas/apache-atlas-'${version}'-impala-hook.tar.gz
