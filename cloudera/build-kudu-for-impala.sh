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

# This script is builds Kudu using Impala's toolchain components. It accomplishes this
# by downloading Impala's toolchain using bootstrap_toolchain.py, then sets up a variety
# of compiler related environment variables and does a full build of Kudu and its
# thirdparty. This script requires IMPALA_HOME to be set. This script also requires the
# following environment variables:
# KUDU_HOME - location of the Kudu source. This will be modified.
# KUDU_FOR_IMPALA_OUTPUT_DIR - destination directory for the Kudu tarball
# The script will generate a tarball named kudu-${IMPALA_KUDU_VERSION}.tar.gz in the
# specified output directory.

set -eux -o pipefail

# This script requires IMPALA_HOME to be set
[[ -n "$IMPALA_HOME" ]]
. ${IMPALA_HOME}/bin/impala-config.sh

# Assert that the required environment variables are set
[[ -n "$KUDU_HOME" ]]
[[ -n "$KUDU_FOR_IMPALA_OUTPUT_DIR" ]]

# Exports various compiler related flags. This was taken from
# native-toolchain/init-compiler.sh and should be kept in sync with it.
function init-compiler() {
  # Enable ccache.

  # Typically, for Kudu builds, we just put /usr/local/lib/ccache on the path,
  # and we're done. However, that would get us the OS compilers. To use the
  # Impala toolchain, we create wrappers.  Adding the gcc bin/ directory to
  # PATH (after /usr/local/lib/ccache but before other compilers) didn't work
  # because something in third-party invokes plain "cc" which doesn't exist in
  # the GCC directory. Using "CC='ccache .../gcc'" doesn't work because the
  # pre-flight check assumes that CC is one binary, rather than two arguments,
  # and that's a reasonable thing for it to assume.
  cat > wrapped-gcc <<EOF
#!/bin/bash
exec ccache $IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/bin/gcc "\$@"
EOF
  cat > wrapped-g++ <<EOF
#!/bin/bash
exec ccache $IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/bin/g++ "\$@"
EOF
  chmod ugo+x wrapped-gcc wrapped-g++
  export CC=$(pwd)/wrapped-gcc
  export CXX=$(pwd)/wrapped-g++

  # Assert we're getting the right compilers, despite ccache
  diff <($CC --version) <($IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/bin/gcc --version)
  diff <($CXX --version) <($IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/bin/g++ --version)

  # Upgrade rpath variable to catch current library location and possible future location
  FULL_RPATH="-Wl,-rpath,$IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/lib64"
  FULL_RPATH="${FULL_RPATH},-rpath,'$$ORIGIN/../lib',-rpath,'$$ORIGIN/../lib64'"
  FULL_LPATH="-L$IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/lib64"

  ARCH_FLAGS="-mno-avx2"
  LDFLAGS="$ARCH_FLAGS $FULL_RPATH $FULL_LPATH"
  CXXFLAGS="$ARCH_FLAGS -fPIC -m64"
  CFLAGS="-fPIC -m64"
  BOOST_ROOT="$IMPALA_TOOLCHAIN/boost-$IMPALA_BOOST_VERSION"

  INCLUDE_PREFIX=$IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/include/c++/$IMPALA_GCC_VERSION/
  CPLUS_INCLUDE_PATH=$INCLUDE_PREFIX:$INCLUDE_PREFIX/x86_64-unknown-linux-gnu/

  # Add binutils, cmake, and gcc to the path.
  PATH="$IMPALA_TOOLCHAIN/binutils-$IMPALA_BINUTILS_VERSION/bin:$PATH"
  PATH="$IMPALA_TOOLCHAIN/cmake-$IMPALA_CMAKE_VERSION/bin/:$PATH"
  PATH="$IMPALA_TOOLCHAIN/gcc-$IMPALA_GCC_VERSION/bin/:$PATH"

  export BOOST_ROOT
  export CC
  export CXX
  export CXXFLAGS
  export LDFLAGS
  export CFLAGS
  export CPLUS_INCLUDE_PATH
  export PATH
}

# This should be called from the Kudu build dir.
function install_kudu {
  INSTALL_DIR=$1

  # This actually only installs the client.
  DESTDIR=$INSTALL_DIR make install

  # Install the binaries, but only the needed stuff. Ignore the test utilities. The list
  # of files below should match the files provided by a parcel.
  rm -rf "$INSTALL_DIR/bin"
  mkdir -p "$INSTALL_DIR/bin"
  pushd bin
  for F in kudu-* ; do
    cp $F "$INSTALL_DIR/bin"
  done
  popd

  # Install the web server resources.
  rm -rf "$INSTALL_DIR/lib/kudu/www"
  mkdir -p "$INSTALL_DIR/lib/kudu"
  cp -r ../../www "$INSTALL_DIR/lib/kudu"
}

# Download the toolchain.
pushd ${IMPALA_HOME}
./infra/python/deps/download_requirements
export DOWNLOAD_CDH_COMPONENTS=false
./bin/bootstrap_toolchain.py
popd

# Enter Kudu directory
cd ${KUDU_HOME}

# Set up build environment.
init-compiler

target_load=${BUILD_THREADS:-$(nproc)}
BUILD_THREADS=${BUILD_THREADS:-$(nproc)}
if [[ ${BUILD_THREADS} -lt 1 ]]; then
  BUILD_THREADS=1
fi
# --load-average does not work with ninja (which Kudu will try to use if it is present),
# so only add --load-average flag if ninja is not available.
KUDU_MAKE_FLAGS=""
if ! which ninja-build > /dev/null && ! which ninja > /dev/null ; then
  KUDU_MAKE_FLAGS+="--load-average=${target_load}"
fi

# enable ccache logging
# ccache itself was enabled in init-compiler by path manipulations
export CCACHE_LOGFILE=${KUDU_HOME}/ccache-log-kudu-for-impala.txt
# CCache will work without additional settings when rebuilding in the same directory
# structure. The CDP build can run in different directories, and it wants to be able to
# import a ccache from one run with a particular base directory and use it in a different
# run with a different base directory. Since this is specific to the CDP build, those
# settings (which are intricate) are set in cdp_install_cmd.sh to avoid complicating
# this script.

ccache --version || true # Dump version, never fail
ccache -s || true   # Dump statistics, never fail.

EXTRA_MAKEFLAGS=${KUDU_MAKE_FLAGS} \
  PARALLEL=${BUILD_THREADS} \
  ./thirdparty/build-if-necessary.sh

# Update the PATH to include Kudu's toolchain binaries.
PATH="`pwd`/thirdparty/installed/common/bin:$PATH"

# Now Kudu can be built.
KUDU_NAME=kudu-${IMPALA_KUDU_VERSION}
for type in release debug; do
  INSTALL_DIR=`pwd`/${KUDU_NAME}/${type}
  mkdir -p build/${type}
  pushd build/${type}
  cmake \
    -DCMAKE_BUILD_TYPE=${type} \
    -DNO_TESTS=1 \
    -DCMAKE_INSTALL_PREFIX="" ../..
  make -j$BUILD_THREADS $KUDU_MAKE_FLAGS
  # We install Kudu twice:
  # - once into the directory structure Impala expects for compilation
  #   (This means that KUDU_CLIENT_DIR can be set to $KUDU_HOME/build/release to
  #    build Impala)
  DESTDIR=`pwd`/usr/local make install
  # - once into the directory structure we want to package the tarballs in
  install_kudu $INSTALL_DIR
  popd
done

# Construct the tarball into KUDU_FOR_IMPALA_OUTPUT_DIR
OUT=${KUDU_FOR_IMPALA_OUTPUT_DIR}
mkdir -p ${OUT}
tar --use-compress-program pigz -cf ${OUT}/${KUDU_NAME}.tar.gz ${KUDU_NAME}

ccache -s || true   # Dump statistics, never fail.
