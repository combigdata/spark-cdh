#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script loads spark-env.sh if it exists, and ensures it is only loaded once.
# spark-env.sh is loaded from SPARK_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

# Figure out where Spark is installed
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

# Save SPARK_HOME in case the user's spark-env.sh overwrites it.
ORIGINAL_SPARK_HOME="$SPARK_HOME"

if [ -z "$SPARK_ENV_LOADED" ]; then
  export SPARK_ENV_LOADED=1

  export SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}"/conf}"

  if [ -f "${SPARK_CONF_DIR}/spark-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${SPARK_CONF_DIR}/spark-env.sh"
    set +a
  fi
fi

# Setting SPARK_SCALA_VERSION if not already set.

if [ -z "$SPARK_SCALA_VERSION" ]; then
  USER_SCALA_VERSION_SET=0
  ASSEMBLY_DIR2="${SPARK_HOME}/assembly/target/scala-2.11"
  ASSEMBLY_DIR1="${SPARK_HOME}/assembly/target/scala-2.12"

  if [[ -d "$ASSEMBLY_DIR2" && -d "$ASSEMBLY_DIR1" ]]; then
    echo -e "Presence of build for multiple Scala versions detected." 1>&2
    echo -e 'Either clean one of them or, export SPARK_SCALA_VERSION in spark-env.sh.' 1>&2
    exit 1
  fi

  if [ -d "$ASSEMBLY_DIR2" ]; then
    export SPARK_SCALA_VERSION="2.11"
  else
    export SPARK_SCALA_VERSION="2.12"
  fi
else
    USER_SCALA_VERSION_SET=1
fi

# Check that the user's SPARK_HOME and the expected SPARK_HOME match. If they don't, issue a
# warning, and delegate execution to the user-defined SPARK_HOME.
if [ "$SPARK_HOME" != "$ORIGINAL_SPARK_HOME" ]; then
  SCRIPT=$(basename "$0")
  echo "WARNING: User-defined SPARK_HOME ($SPARK_HOME) overrides detected ($ORIGINAL_SPARK_HOME)." 1>&2
  echo "WARNING: Running $SCRIPT from user-defined location." 1>&2
  if [ $USER_SCALA_VERSION_SET = 0 ]; then
    unset SPARK_SCALA_VERSION
  fi
  unset SPARK_ENV_LOADED
  exec "$SPARK_HOME/bin/$SCRIPT" "$@"
fi
