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
#
# This script is called from the impala install_cmd code in CDPD/components.ini.
# It is always invoked from the Impala directory.
# Inputs:
#  - TMP_PACKAGES_DIR, which has tarballs for Hive source, Hadoop binary, Kudu source,
#    and Kudu binary
#  - IMPALA_VERSION, which is the version string for Impala
# Outputs (placed in IMPALA_HOME):
#  - kudu-for-impala-${IMPALA_KUDU_VERSION}.tar.gz
#  - kudu-jars-${IMPALA_KUDU_VERSION}.tar.gz
#  - impala-${IMPALA_VERSION}.tar.gz
#  - ccache-log-kudu-for-impala.txt
#  - ccache-log-impala.txt
set -euo pipefail
# Use extensive logging for now
set -x

# Do a sanity check to make sure the input environment variables are set to
# non-empty values.
[[ -n "${TMP_PACKAGES_DIR}" ]]
[[ -n "${IMPALA_VERSION}" ]]

export IMPALA_HOME=$(pwd)
export SOURCE_ROOT="${IMPALA_HOME}/.."
export DOWNLOAD_CDH_COMPONENTS=false
export PYPI_MIRROR='https://pypi.infra.cloudera.com/api/pypi/pypi-public'
export IS_STANDALONE_IMPALA_BUILD=false

# unpack_into_directory TARBALL OUTPUT_DIR
# Unpacks TARBALL, stripping the top level directory and outputting to OUTPUT_DIR
function unpack_into_directory {
  TARBALL=$1
  OUTPUT_DIR=$2
  tar -zxf ${TARBALL} --strip-components=1 -C ${OUTPUT_DIR}
}

# Sourcing bin/impala-config.sh gets us all the versions (set by the text-replace section
# in CDPD/components.ini).
. ${IMPALA_HOME}/bin/impala-config.sh

# Now we have all the appropriate versions and can verify the tarballs exist
HIVE_SOURCE_TARBALL="${TMP_PACKAGES_DIR}/hive-${IMPALA_HIVE_VERSION}-source.tar.gz"
HADOOP_BINARY_TARBALL="${TMP_PACKAGES_DIR}/hadoop-${IMPALA_HADOOP_VERSION}.tar.gz"
KUDU_SOURCE_TARBALL="${TMP_PACKAGES_DIR}/kudu-${IMPALA_KUDU_VERSION}-source.tar.gz"
KUDU_BINARY_TARBALL="${TMP_PACKAGES_DIR}/kudu-${IMPALA_KUDU_VERSION}.tar.gz"
if [[ ! -f "${HIVE_SOURCE_TARBALL}" || ! -f "${HADOOP_BINARY_TARBALL}" ||
      ! -f "${KUDU_SOURCE_TARBALL}" || ! -f "${KUDU_BINARY_TARBALL}" ]]; then
    echo "Expecting the following files:"
    echo "${HIVE_SOURCE_TARBALL}"
    echo "${HADOOP_BINARY_TARBALL}"
    echo "${KUDU_SOURCE_TARBALL}"
    echo "${KUDU_BINARY_TARBALL}"
    echo "Found:"
    ls -l "${TMP_PACKAGES_DIR}"
    exit 1
fi

# The runtime for this build is highly impacted by ccache, so dump various information
# about ccache (version, config) and disk usage to give an idea of the effectiveness
# of ccache.
# A run with a very large ccache ended using about 11GB in the cache, so set the ccache
# to 15GB to give some headroom.
ccache --max-size=15G || true
ccache --version || true
ccache -p || true
df || true
# The CDP build prepopulates the ccache directory. Zero ccache statistics to ignore
# this irrelevent past.
ccache -z || true
# The CDP build can run in different directories for different Jenkins jobs. To share
# the ccache between them, the directory name needs to be excluded from the ccache hash.
# There are two ccache options needed to handle the different directories:
# 1. CCACHE_BASEDIR corrects absolute paths in the compilation commands.
#    It is important that this correction work for the compiler as well as the code.
#    For Kudu compilation, the code being compiled is inside KUDU_HOME, but the compiler
#    is in IMPALA_TOOLCHAIN (inside IMPALA_HOME). CCACHE_BASEDIR needs to correct both,
#    so it needs to be a common directory above both KUDU_HOME and IMPALA_HOME. So, this
#    is set to SOURCE_ROOT. This handles the differences between the different Jenkins
#    jobs. This setting also works for Impala compilation.
# 2. CCACHE_NOHASHDIR tells ccache not to hash the directory name. Set this to true.
#    The implication of ignoring the directory is that the wrong directory name can be
#    embedded in the binary (e.g. in the debug info). This should be harmless, since the
#    tools that use it (gdb) have ways of overriding it (and the existing directory is
#    already bogus).
# These settings work for all the compilation (Kudu and Impala), so set this once for the
# whole script.
export CCACHE_BASEDIR="$(cd ${SOURCE_ROOT} && pwd)"
export CCACHE_NOHASHDIR="true"

# Step 1: Build Kudu with Impala's native toolchain
echo "### Building Kudu with Impala's native toolchain ###"
export KUDU_HOME="${SOURCE_ROOT}/kudu_for_impala"
mkdir -p ${KUDU_HOME}
unpack_into_directory "$KUDU_SOURCE_TARBALL" ${KUDU_HOME}
export DEPENDENCY_URL='http://cloudera-thirdparty-libs.s3.amazonaws.com'
export KUDU_FOR_IMPALA_OUTPUT_DIR="${KUDU_HOME}"
time ./cloudera/build-kudu-for-impala.sh
unset DEPENDENCY_URL
unset KUDU_FOR_IMPALA_OUTPUT_DIR
# Point the Impala build at the Kudu we just built.
# Impala requires that if KUDU_CLIENT_DIR is set, KUDU_BUILD_DIR must also be, but
# KUDU_BUILD_DIR is only used when running the minicluster, so it doesn't matter here.
export KUDU_CLIENT_DIR="${KUDU_HOME}/build/release"
export KUDU_BUILD_DIR="/"

# Step 2: Unpack Hive, Hadoop dependency tarballs. These are used to satisfy Impala's
#         backend C++/thrift dependencies
echo "### Unpacking Hive, Hadoop dependency tarballs ###"
mkdir ${SOURCE_ROOT}/hive ${SOURCE_ROOT}/hadoop
unpack_into_directory "$HIVE_SOURCE_TARBALL" ${SOURCE_ROOT}/hive
unpack_into_directory "$HADOOP_BINARY_TARBALL" ${SOURCE_ROOT}/hadoop
export HIVE_SRC_DIR_OVERRIDE="${SOURCE_ROOT}/hive"
export HADOOP_INCLUDE_DIR_OVERRIDE="${SOURCE_ROOT}/hadoop/include"
export HADOOP_LIB_DIR_OVERRIDE="${SOURCE_ROOT}/hadoop/lib"

# Step 3: Build Impala
echo "### Building Impala ###"
ccache -s || true
ccache -z || true
# Hack: Impala builds have seen g++ getting killed due to running out of memory. As a
# temporary workaround, reduce parallelism to 3/4 normal. The machines are running
# in a Kubernetes pod with a CPU limit below the number of CPUs, so this may not
# impact build time. Kudu compilation isn't seeing this, so restrict this to Impala
# for now.
IMPALA_BUILD_THREADS=${IMPALA_BUILD_THREADS:-(($(nproc) * 3 / 4))}
echo "Set IMPALA_BUILD_THREADS to ${IMPALA_BUILD_THREADS}"

# Log ccache accesses to help diagnose bad cache hit rates
# (Needs to be an absolute path, because ccache is invoked in many different directories)
export CCACHE_LOGFILE="${IMPALA_HOME}/ccache-log-impala-build.txt"
time ./buildall.sh -noclean -notests -release_and_debug
ccache -s || true
df || true

# Step 4: Build testdata (to make sure it builds)
echo "### Building hbase splitter ###"
pushd testdata
mvn-quiet.sh clean
mvn-quiet.sh package
popd

# Step 5: Copy Kudu client libraries into ${IMPALA_HOME}/kudu/build/release
echo "### Copying Kudu client libraries into the right place ###"
mkdir -p ${IMPALA_HOME}/kudu/build/release
for file in $(find ${KUDU_HOME}/build/release/ -name "libkudu_client.so.*"); do
    cp -L ${file} ${IMPALA_HOME}/kudu/build/release
done;

# Step 6: Build Impala tarball
echo "### Building Impala tarball ###"
tar --use-compress-program pigz -cf ../impala-${IMPALA_VERSION}.tar.gz . \
    --exclude-vcs --exclude=impala-${IMPALA_VERSION}.tar.gz \
    --exclude=testdata/cluster/cdh*
mv ../impala-${IMPALA_VERSION}.tar.gz .

# Step 7: Move Kudu-for-Impala tarball and ccache log to the right place
echo "### Putting Kudu-for-Impala tarball / ccache log in the right place ###"
mv ${KUDU_HOME}/kudu-${IMPALA_KUDU_VERSION}.tar.gz \
   ./kudu-for-impala-${IMPALA_KUDU_VERSION}.tar.gz
mv ${KUDU_HOME}/ccache-log-kudu-for-impala.txt .

# Step 8: Make Kudu jars tarball by copying kudu-hive jars out of the regular Kudu
#         binary tarball
echo "### Building Kudu-jars tarball ###"
KUDU_BINARY_HOME=${SOURCE_ROOT}/kudu_binary
KUDU_JARS_DIR=${SOURCE_ROOT}/kudu-jars-${IMPALA_KUDU_VERSION}
mkdir -p ${KUDU_BINARY_HOME} ${KUDU_JARS_DIR}
unpack_into_directory "$KUDU_BINARY_TARBALL" ${KUDU_BINARY_HOME}
cp ${KUDU_BINARY_HOME}/java/kudu-hive*.jar ${KUDU_JARS_DIR}
tar --use-compress-program pigz -cf ./kudu-jars-${IMPALA_KUDU_VERSION}.tar.gz \
    ${KUDU_JARS_DIR}

# Step 9: make docker images (only for Redhat/Centos 7)
if [[ -f /etc/redhat-release ]] && grep 'release 7\.' /etc/redhat-release; then
    echo "### This is Redhat/Centos 7: building docker images ###"
    make docker_images
else
    echo "### This is not Redhat/Centos 7: skipping build of docker images ###"
fi;
