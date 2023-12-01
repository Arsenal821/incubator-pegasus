#!/bin/bash
#
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

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT=$(dirname "${SCRIPT_DIR}")
# This file should be sourced to set up LD_LIBRARY_PATH and CLASSPATH to
# run Pegasus binaries which use libhdfs in the context of a dev environment.

# Try to detect the system's JAVA_HOME
# If javac exists, then the system has a Java SDK (JRE does not have javac).
# Follow the symbolic links and use this to determine the system's JAVA_HOME.
SYSTEM_JAVA_HOME="/usr/java/default"
if [ -n "$(which javac)" ]; then
  SYSTEM_JAVA_HOME=$(which javac | xargs readlink -f | sed "s:/bin/javac::")
fi
# Prefer the JAVA_HOME set in the environment, but use the system's JAVA_HOME otherwise.
export JAVA_HOME="${JAVA_HOME:-${SYSTEM_JAVA_HOME}}"
if [ ! -d "$JAVA_HOME" ]; then
  echo "JAVA_HOME must be set to the location of your JDK!"
  return 1
fi
# Link jvm library.
JAVA_JVM_LIBRARY_DIR=$(dirname $(find "${JAVA_HOME}/" -name libjvm.so  | head -1))
export LD_LIBRARY_PATH=${JAVA_JVM_LIBRARY_DIR}:$LD_LIBRARY_PATH

if [ ! -d "$HADOOP_HOME" ]; then
  TOOLCHAIN_VERSION="toolchain-4.0.x-20231128"
  HADOOP_VERSION="hadoop-3.1.1.7.2.9.0-146"
  HADOOP_PACKAGE=${TOOLCHAIN_VERSION}-${HADOOP_VERSION}-"$(arch)"-el9.tgz
  if [ "$(arch)" = "x86_64" ]; then
    HADOOP_PACKAGE_MD5="a040be28e2f42eebea2db2b4cf147932"
  elif [ "$(arch)" = "aarch64" ]; then
    HADOOP_PACKAGE_MD5="50a5e4481dc1ef65f3553ee1c9753efd"
  else
    echo "ERROR: unsupported arch $(arch)"
    return 1
  fi
  PEGASUS_HADOOP_HOME=${ROOT}/"${HADOOP_VERSION}"
  if [ ! -d "${PEGASUS_HADOOP_HOME}" ]; then
    if [ ! -f "${ROOT}/${HADOOP_PACKAGE}" ]; then
      echo "Downloading hadoop..."
      download_url="https://jfrog-internal.sensorsdata.cn:443/artifactory/dragon-internal/inf/skv/${HADOOP_PACKAGE}"
      if ! wget -T 10 -t 5 "$download_url"; then
        echo "ERROR: download hadoop failed"
        return 1
      fi
    fi
    if [ "$(md5sum "${HADOOP_PACKAGE}" | awk '{print$1}')" != "${HADOOP_PACKAGE_MD5}" ]; then
      echo "check file ${HADOOP_PACKAGE} md5sum failed!"
      return 1
    fi

    echo "Decompressing HDFS..."
    if ! tar xf "${HADOOP_PACKAGE}"; then
      echo "ERROR: decompress hadoop failed"
      return 1
    fi
  fi

  # Set the HADOOP_HOME to the pegasus's hadoop directory.
  export HADOOP_HOME="${PEGASUS_HADOOP_HOME}"
  echo "set HADOOP_HOME to ${PEGASUS_HADOOP_HOME}"
fi

# Set CLASSPATH to all the Hadoop jars needed to run Hadoop itself as well as
# the right configuration directory containing core-site.xml or hdfs-site.xml.
if [ ! -d "$HADOOP_HOME/etc/hadoop" ] || [ ! -d "$HADOOP_HOME/share/hadoop" ]; then
  echo "HADOOP_HOME must be set to the location of your Hadoop jars and core-site.xml."
  return 1
fi
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop/
for f in $HADOOP_HOME/share/hadoop/common/lib/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done
for f in $HADOOP_HOME/share/hadoop/common/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done
for f in $HADOOP_HOME/share/hadoop/hdfs/lib/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done
for f in $HADOOP_HOME/share/hadoop/hdfs/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done
