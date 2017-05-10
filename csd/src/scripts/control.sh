#!/bin/bash
##
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

. $(cd $(dirname $0) && pwd)/common.sh

CSD_VERSION=$(cat "$CONF_DIR/meta/version")

# Drop the "-SNAPSHOT" from the name since we don't put that in the parcel / package version.
CSD_VERSION=${CSD_VERSION/-SNAPSHOT/}

SPARK_VERSION_FILE="$SPARK_HOME/cloudera/spark2_version.properties"
SPARK_VERSION=$(read_property "version" "$SPARK_VERSION_FILE")
SPARK_VERSION_WITHOUT_SNAPSHOT=${SPARK_VERSION/-SNAPSHOT/}
if [ "$CSD_VERSION" != "$SPARK_VERSION_WITHOUT_SNAPSHOT" ]; then
  echo "**********"
  echo "* The CSD version ($CSD_VERSION) is not compatible with the current"
  echo "* Spark 2 version ($SPARK_VERSION_WITHOUT_SNAPSHOT). Either update your CSD or      "
  echo "* make sure you have the correct Spark version installed and       "
  echo "* activated.                                                       "
  echo "**********"
  exit 1
fi

case $1 in
  (start_history_server)
    start_history_server
    ;;

  (client)
    deploy_client_config
    ;;

  (*)
    log "Don't understand [$1]"
    exit 1
    ;;
esac
