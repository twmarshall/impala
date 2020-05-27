#!/usr/bin/env impala-python
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

# Assembles the artifacts required to build docker containers into a single directory.
# Most artifacts are symlinked so need to be dereferenced (e.g. with tar -h) before
# being used as a build context.

import argparse
import glob
import os
import shutil
from subprocess import check_call
from urllib2 import urlopen
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("--debug-build", help="Setup build context for debug build",
                    action="store_true")
args = parser.parse_args()

IMPALA_HOME = os.environ["IMPALA_HOME"]
if args.debug_build:
  BUILD_TYPE = "debug"
else:
  BUILD_TYPE = "release"
OUTPUT_DIR = os.path.join(IMPALA_HOME, "docker/build_context", BUILD_TYPE)
DOCKERFILE = os.path.join(IMPALA_HOME, "docker/impala_base/Dockerfile")

IMPALA_TOOLCHAIN_PACKAGES_HOME = os.environ["IMPALA_TOOLCHAIN_PACKAGES_HOME"]
IMPALA_GCC_VERSION = os.environ["IMPALA_GCC_VERSION"]
IMPALA_BINUTILS_VERSION = os.environ["IMPALA_BINUTILS_VERSION"]
GCC_HOME = os.path.join(IMPALA_TOOLCHAIN_PACKAGES_HOME,
    "gcc-{0}".format(IMPALA_GCC_VERSION))
BINUTILS_HOME = os.path.join(
    IMPALA_TOOLCHAIN_PACKAGES_HOME, "binutils-{0}".format(IMPALA_BINUTILS_VERSION))
STRIP = os.path.join(BINUTILS_HOME, "bin/strip")
KUDU_HOME = os.environ["IMPALA_KUDU_HOME"]
KUDU_CLIENT_DIR = os.environ.get("KUDU_CLIENT_DIR")
# Different distros put Kudu libraries in different places.
# For now we ensure that we at least have the RHEL7 locations used in the CDP build.
# An example of the locations of the .so files are as follows:
# find -name *.so
# ./kudu-1.9.0.7.0.0.0-294/build/release/client/usr/local/lib64/libkudu_client.so
# ./kudu-1.9.0.7.0.0.0-294/build/release/lib/exported/libkudu_client.so
# TODO: clean this up upstream to gracefully handle multiple distros
if KUDU_CLIENT_DIR:
  kudu_lib_dirs = [os.path.join(KUDU_CLIENT_DIR, "usr/local/lib"),
                   os.path.join(KUDU_CLIENT_DIR, "usr/local/lib64")]
else:
  kudu_lib_dirs = [os.path.join(KUDU_HOME, "release/lib"),
                   os.path.join(KUDU_HOME, "release/lib64"),
                   os.path.join(KUDU_HOME, "release/lib/exported")]

# Ensure the output directory exists and is empty.
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

os.symlink(os.path.relpath(DOCKERFILE, OUTPUT_DIR),
    os.path.join(OUTPUT_DIR, "Dockerfile"))

BIN_DIR = os.path.join(OUTPUT_DIR, "bin")
LIB_DIR = os.path.join(OUTPUT_DIR, "lib")
os.mkdir(BIN_DIR)
os.mkdir(LIB_DIR)


def symlink_file_into_dir(src_file, dst_dir):
  """Helper to symlink 'src_file' into 'dst_dir'."""
  os.symlink(src_file, os.path.join(dst_dir, os.path.basename(src_file)))

# Impala binaries and native dependencies.

# Strip debug symbols from release build to reduce image size. Keep them for
# debug build.
IMPALAD_BINARY = os.path.join(IMPALA_HOME, "be/build", BUILD_TYPE, "service/impalad")
if args.debug_build:
  symlink_file_into_dir(IMPALAD_BINARY, BIN_DIR)
else:
  check_call([STRIP, "--strip-debug", IMPALAD_BINARY,
              "-o", os.path.join(BIN_DIR, "impalad")])
for lib in ["libstdc++", "libgcc"]:
  for so in glob.glob(os.path.join(GCC_HOME, "lib64/{0}*.so*".format(lib))):
    symlink_file_into_dir(so, LIB_DIR)

found_kudu_so = False
for kudu_lib_dir in kudu_lib_dirs:
  for so in glob.glob(os.path.join(kudu_lib_dir, "libkudu_client.so*")):
    found_kudu_so = True
    symlink_file_into_dir(so, LIB_DIR)

if not found_kudu_so:
  raise Exception("No Kudu shared object found in search path: {0}".format(kudu_lib_dirs))

# Impala dependencies.
dep_classpath = file(os.path.join(IMPALA_HOME, "fe/target/build-classpath.txt")).read()
for jar in dep_classpath.split(":"):
  assert os.path.exists(jar), "missing jar from classpath: {0}".format(jar)
  symlink_file_into_dir(jar, LIB_DIR)

# Impala jars.
for jar in glob.glob(
    os.path.join(IMPALA_HOME, "fe/target/impala-frontend-*-SNAPSHOT.jar")):
  symlink_file_into_dir(jar, LIB_DIR)

# Extract Atlas JARS and copy to LIB_DIR.
ATLAS_HOOK_DIR = os.environ["ATLAS_HOOK_DIR"]
# Create a directory for atlas plugin implementation jars.
ATLAS_IMPALA_IMPL_DIR = os.path.join(LIB_DIR, "atlas-impala-plugin-impl")
os.mkdir(ATLAS_IMPALA_IMPL_DIR)
try:
  bridge_jars = glob.glob("{0}/hook/impala/*.jar".format(ATLAS_HOOK_DIR))
  assert len(bridge_jars) > 0, "No Atlas jars found in /hook/impala/ directory."
  for jar in bridge_jars:
    shutil.copy(jar, LIB_DIR)
  impl_jars = glob.glob("{0}/hook/impala/atlas-impala-plugin-impl/*.jar"
      .format(ATLAS_HOOK_DIR))
  assert len(impl_jars) > 0, "No Atlas jars found in hook/impala/" \
    "atlas-impala-plugin-impl directory."
  for jar in impl_jars:
    shutil.copy(jar, ATLAS_IMPALA_IMPL_DIR)
except Exception as e:
  raise Exception("Unable to copy Atlas jars: {0}".format(e.message))

# Templates for debug web pages.
os.symlink(os.path.join(IMPALA_HOME, "www"), os.path.join(OUTPUT_DIR, "www"))
# Scripts
symlink_file_into_dir(os.path.join(IMPALA_HOME, "docker/daemon_entrypoint.sh"), BIN_DIR)
symlink_file_into_dir(os.path.join(IMPALA_HOME, "bin/graceful_shutdown_backends.sh"),
                      BIN_DIR)
