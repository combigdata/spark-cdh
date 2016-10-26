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

#
# Set of utility functions shared across different Spark CSDs.
#

set -ex

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Time marker for both stderr and stdout
log "Running Spark2 CSD control script..."
log "Detected CDH_VERSION of [$CDH_VERSION]"

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

export HADOOP_HOME=${HADOOP_HOME:-$(readlink -m "$CDH_HADOOP_HOME")}
export HADOOP_BIN=$HADOOP_HOME/bin/hadoop
export HDFS_BIN=$HADOOP_HOME/../../bin/hdfs
export HADOOP_CONF_DIR="$CONF_DIR/yarn-conf"
export HIVE_CONF_DIR="$CONF_DIR/hive-conf"

# If SPARK2_HOME is not set, make it the default
DEFAULT_SPARK2_HOME=/usr/lib/spark2
SPARK_HOME=${SPARK2_HOME:-${CDH_SPARK2_HOME:-$DEFAULT_SPARK2_HOME}}
export SPARK_HOME=$(readlink -m "${SPARK_HOME}")

# We want to use a local conf dir
export SPARK_CONF_DIR="$CONF_DIR/spark2-conf"
if [ ! -d "$SPARK_CONF_DIR" ]; then
  mkdir "$SPARK_CONF_DIR"
fi

# Variables used when generating configs.
export SPARK_ENV="$SPARK_CONF_DIR/spark-env.sh"
export SPARK_DEFAULTS="$SPARK_CONF_DIR/spark-defaults.conf"

# Set JAVA_OPTS for the daemons
# sets preference to IPV4
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Djava.net.preferIPv4Stack=true"

# Make sure PARCELS_ROOT is in the format we expect, canonicalized and without a trailing slash.
export PARCELS_ROOT=$(readlink -m "$PARCELS_ROOT")

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function get_hadoop_conf {
  local conf="$1"
  local key="$2"
  "$HDFS_BIN" --config "$conf" getconf -confKey "$key"
}

function get_default_fs {
  get_hadoop_conf "$1" "fs.defaultFS"
}

# replace $1 with $2 in file $3
function replace {
  perl -pi -e "s#${1}#${2}#g" $3
}

# Read a value from a properties file.
function read_property {
  local key="$1"
  local file="$2"
  echo $(grep "^$key=" "$file" | tail -n 1 | sed "s/^$key=\(.*\)/\\1/")
}

# Replaces a configuration in the Spark config with a new value; keeps just
# one entry for the configuration (in case the value is defined multiple times
# because of safety valves).
function replace_spark_conf {
  local key="$1"
  local value="$2"
  local file="$3"
  local temp="$file.tmp"
  grep -v "^$key=" "$file" > "$temp"
  echo "$key=$value" >> "$temp"
  mv "$temp" "$file"
}

# Prepend a given protocol string to the URL if it doesn't have a protocol.
function prepend_protocol {
  local url="$1"
  local proto="$2"
  if [[ "$url" =~ [:alnum:]*:.* ]]; then
    echo "$url"
  else
    echo "$proto$url"
  fi
}

# Adds jars in classpath entry $2 to the classpath file $1, resolving symlinks so that duplicates
# can be removed. Jars already present in Spark's distribution are also ignored. See CDH-27596.
function add_to_classpath {
  local CLASSPATH_FILE="$1"
  local CLASSPATH="$2"
  local SPARK_JARS="$SPARK2_HOME/jars"

  # Break the classpath into individual entries
  IFS=: read -a CLASSPATH_ENTRIES <<< "$CLASSPATH"

  for pattern in "${CLASSPATH_ENTRIES[@]}"; do
    for entry in $pattern; do
      entry=$(readlink -m "$entry")
      name=$(basename $entry)
      if [ -f "$entry" ] && [ ! -f "$SPARK_HOME/$name" ] && ! grep -q "/$name\$" "$CLASSPATH_FILE"
      then
        echo "$entry" >> "$CLASSPATH_FILE"
      fi
    done
  done
}

# Prepare the spark-env.sh file specified in $1 for use.
function prepare_spark_env {
  local client="$1"
  replace "{{HADOOP_HOME}}" "$HADOOP_HOME" "$SPARK_ENV"
  replace "{{SPARK_HOME}}" "$SPARK_HOME" "$SPARK_ENV"
  replace "{{SPARK_EXTRA_LIB_PATH}}" "$SPARK_LIBRARY_PATH" "$SPARK_ENV"
  replace "{{PYTHON_PATH}}" "$PYTHON_PATH" "$SPARK_ENV"
  replace "{{CDH_PYTHON}}" "$CDH_PYTHON" "$SPARK_ENV"

  local CLASSPATH_FILE="$(dirname $SPARK_ENV)/classpath.txt"
  local CLASSPATH_FILE_TMP="${CLASSPATH_FILE}.tmp"
  local HADOOP_CLASSPATH=$($HADOOP_BIN --config "$HADOOP_CONF_DIR" classpath)
  add_to_classpath "$CLASSPATH_FILE_TMP" "$HADOOP_CLASSPATH"

  # De-duplicate the classpath when creating the target file.
  cat "$CLASSPATH_FILE_TMP" | sort | uniq > "$CLASSPATH_FILE"
  rm -f "$CLASSPATH_FILE_TMP"
}

# Check whether the given config key ($1) exists in the given conf file ($2).
function has_config {
  local key="$1"
  local file="$2"
  grep -q "^$key=" "$file"
}

# Set a configuration key ($1) to a value ($2) in the file ($3) only if it hasn't already been
# set by the user.
function set_config {
  local key="$1"
  local value="$2"
  local file="$3"
  if ! has_config "$key" "$file"; then
    echo "$key=$value" >> "$file"
  fi
}

# Copies config files from a source directory ($1) into the Spark client config dir ($2).
# $3 should be the final location of the client config. Ignores logging configuration, and
# does not overwrite files, so that multiple source config directories can be merged.
function copy_client_config {
  local source_dir="$1"
  local target_dir="$2"
  local dest_dir="$3"

  for i in "$source_dir"/*; do
    if [ $(basename "$i") != log4j.properties ]; then
      mv $i "$target_dir"

      # CDH-28425. Because of OPSAPS-25695, we need to fix the YARN config ourselves.
      target="$target_dir/$(basename $i)"
      replace "{{CDH_MR2_HOME}}" "$CDH_MR2_HOME" "$target"
      replace "{{HADOOP_CLASSPATH}}" "" "$target"
      replace "{{JAVA_LIBRARY_PATH}}" "" "$target"
      replace "{{CMF_CONF_DIR}}" "$dest_dir" "$target"
    fi
  done
}

function run_spark_class {
  local ARGS=($@)
  ARGS+=($ADDITIONAL_ARGS)
  prepare_spark_env
  export SPARK_DAEMON_JAVA_OPTS="$CSD_JAVA_OPTS $SPARK_DAEMON_JAVA_OPTS"
  export SPARK_JAVA_OPTS="$CSD_JAVA_OPTS $SPARK_JAVA_OPTS"
  cmd="$SPARK_HOME/bin/spark-class ${ARGS[@]}"
  echo "Running [$cmd]"
  exec $cmd
}

function start_history_server {
  log "Starting Spark History Server"
  local CONF_FILE="$SPARK_CONF_DIR/spark-history-server.conf"
  local DEFAULT_FS=$(get_default_fs $HADOOP_CONF_DIR)
  local LOG_DIR=$(prepend_protocol "$HISTORY_LOG_DIR" "$DEFAULT_FS")

  echo "spark.history.fs.logDirectory=$LOG_DIR" >> "$CONF_FILE"
  if [ "$SPARK_PRINCIPAL" != "" ]; then
    echo "spark.history.kerberos.enabled=true" >> "$CONF_FILE"
    echo "spark.history.kerberos.principal=$SPARK_PRINCIPAL" >> "$CONF_FILE"
    echo "spark.history.kerberos.keytab=spark2_on_yarn.keytab" >> "$CONF_FILE"
  fi

  ARGS=(
    "org.apache.spark.deploy.history.HistoryServer"
    "--properties-file"
    "$CONF_FILE"
  )
  run_spark_class "${ARGS[@]}"
}

function deploy_client_config {
  log "Deploying client configuration"

  prepare_spark_env "client"

  set_config 'spark.master' 'yarn' "$SPARK_DEFAULTS"
  set_config 'spark.submit.deployMode' "$DEPLOY_MODE" "$SPARK_DEFAULTS"

  if [ -n "$PYTHON_PATH" ]; then
    echo "spark.executorEnv.PYTHONPATH=$PYTHON_PATH" >> $SPARK_DEFAULTS
  fi

  # Move the Yarn configuration under the Spark config. Do not overwrite Spark's log4j config.
  HADOOP_CONF_NAME=$(basename "$HADOOP_CONF_DIR")
  HADOOP_CLIENT_CONF_DIR="$SPARK_CONF_DIR/$HADOOP_CONF_NAME"
  TARGET_HADOOP_CONF_DIR="$DEST_PATH/$HADOOP_CONF_NAME"

  mkdir "$HADOOP_CLIENT_CONF_DIR"
  copy_client_config "$HADOOP_CONF_DIR" "$HADOOP_CLIENT_CONF_DIR" "$TARGET_HADOOP_CONF_DIR"

  # If there is a Hive configuration directory, then copy all the extra files into the Hadoop
  # conf dir - so that Spark automatically distributes them - and update the configuration to
  # enable the use of the Hive metastore.
  local catalog_impl='in-memory'
  if [ -d "$HIVE_CONF_DIR" ]; then
    local hive_metastore_jars="\${env:HADOOP_COMMON_HOME}/../hive/lib/*"
    hive_metastore_jars="$hive_metastore_jars:\${env:HADOOP_COMMON_HOME}/client/*"
    set_config 'spark.sql.hive.metastore.jars' "$hive_metastore_jars" "$SPARK_DEFAULTS"
    set_config 'spark.sql.hive.metastore.version' '1.1.0' "$SPARK_DEFAULTS"
    copy_client_config "$HIVE_CONF_DIR" "$HADOOP_CLIENT_CONF_DIR" "$TARGET_HADOOP_CONF_DIR"
    catalog_impl='hive'
  fi
  set_config 'spark.sql.catalogImplementation' "$catalog_impl" "$SPARK_DEFAULTS"

  DEFAULT_FS=$(get_default_fs "$HADOOP_CLIENT_CONF_DIR")

  # SPARK 1.1 makes "file:" the default protocol for the location of event logs. So we need
  # to fix the configuration file to add the protocol. But if the user has specified a path
  # with a protocol, don't overwrite it.
  key="spark.eventLog.dir"
  value=$(read_property "$key" "$SPARK_DEFAULTS")
  if [ -n "$value" ]; then
    value=$(prepend_protocol "$value" "$DEFAULT_FS")
    replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"
  fi

  # If a history server is configured, set its address in the default config file so that
  # the Yarn RM web ui links to the history server for Spark apps.
  HISTORY_PROPS="$SPARK_CONF_DIR/history2.properties"
  HISTORY_HOST=
  if [ -f "$HISTORY_PROPS" ]; then
    for line in $(cat "$HISTORY_PROPS")
    do
      readconf "$line"
      case $key in
       (spark.history.ui.port)
         HISTORY_HOST="$host"
         HISTORY_PORT="$value"
       ;;
      esac
    done
    if [ -n "$HISTORY_HOST" ]; then
      echo "spark.yarn.historyServer.address=http://$HISTORY_HOST:$HISTORY_PORT" >> \
        "$SPARK_DEFAULTS"
    fi
    rm "$HISTORY_PROPS"
  fi

  # If no Spark jars are defined, look for the location of jars on the local filesystem,
  # which we assume will be the same across the cluster.
  key="spark.yarn.jars"
  value=$(read_property "$key" "$SPARK_DEFAULTS")
  if [ -n "$value" ]; then
    local prefixed_values=
    # the value is a comma separated list of files (which can be globs)
    # Where needed, let's prefix it with the FS scheme.
    IFS=',' read -ra VALUES <<< "$value"
    for i in "${VALUES[@]}"; do
      # Add the new entry to it, and then add a comma too.
      prefixed_values=${prefixed_values}$(prepend_protocol "$i" "$DEFAULT_FS")","
    done
    # Take out the last extra comma at the end, if it exists
    prefixed_values=$(sed 's/,$//' <<< $prefixed_values)
  else
    value="local:$SPARK_HOME/jars/*"
  fi
  replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"

  # Set the default library paths for drivers and executors.
  EXTRA_LIB_PATH="$HADOOP_HOME/lib/native"
  if [ -n "$SPARK_LIBRARY_PATH" ]; then
    EXTRA_LIB_PATH="$EXTRA_LIB_PATH:$SPARK_LIBRARY_PATH"
  fi
  for i in driver executor yarn.am; do
    key="spark.${i}.extraLibraryPath"
    value=$(read_property "$key" "$SPARK_DEFAULTS")
    if [ -n "$value" ]; then
      value="$value:$EXTRA_LIB_PATH"
    else
      value="$EXTRA_LIB_PATH"
    fi
    replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"
  done

  # Override the YARN / MR classpath configs since we already include them when generating
  # SPARK_DIST_CLASSPATH. This avoids having the same paths added to the classpath a second
  # time and wasting file descriptors.
  replace_spark_conf "spark.hadoop.mapreduce.application.classpath" "" "$SPARK_DEFAULTS"
  replace_spark_conf "spark.hadoop.yarn.application.classpath" "" "$SPARK_DEFAULTS"

  # If using parcels, write extra configuration that tells Spark to replace the parcel
  # path with references to the NM's environment instead, so that users can have different
  # paths on each node.
  if [ -n "$PARCELS_ROOT" ]; then
    echo "spark.yarn.config.gatewayPath=$PARCELS_ROOT" >> "$SPARK_DEFAULTS"
    echo "spark.yarn.config.replacementPath={{HADOOP_COMMON_HOME}}/../../.." >> "$SPARK_DEFAULTS"
  fi

  if [ -n "$CDH_PYTHON" ]; then
    echo "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$CDH_PYTHON" >> "$SPARK_DEFAULTS"
    echo "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$CDH_PYTHON" >> "$SPARK_DEFAULTS"
  fi

  # Modify the YARN configuration to use the topology script from Spark's config directory.
  # Also need to manually fix the permissions so that the script is executable.
  # If the config doesn't exist (e.g. for standalone, which depends on HDFS and not YARN),
  # we need to add the "|| true" hack to avoid an error in the caller (because of set -e).
  OLD_TOPOLOGY_SCRIPT=$(get_hadoop_conf "$HADOOP_CLIENT_CONF_DIR" "net.topology.script.file.name" || true)
  if [ -n "$OLD_TOPOLOGY_SCRIPT" ]; then
    SCRIPT_NAME=$(basename "$OLD_TOPOLOGY_SCRIPT")
    NEW_TOPOLOGY_SCRIPT="$TARGET_HADOOP_CONF_DIR/$SCRIPT_NAME"
    CORE_SITE="$HADOOP_CLIENT_CONF_DIR/core-site.xml"
    chmod 755 "$HADOOP_CLIENT_CONF_DIR/$SCRIPT_NAME"
    sed "s,$OLD_TOPOLOGY_SCRIPT,$NEW_TOPOLOGY_SCRIPT," "$CORE_SITE" > "$CORE_SITE.tmp"
    mv "$CORE_SITE.tmp" "$CORE_SITE"
  fi

  # These values cannot be declared in the descriptor, since the CSD framework will
  # treat them as config references and fail. So add them here unless they've already
  # been set by the user in the safety valve.
  LOG_CONFIG="$SPARK_CONF_DIR/log4j.properties"
  SHELL_CLASSES=("org.apache.spark.repl.Main"
    "org.apache.spark.api.python.PythonGatewayServer")
  for class in "${SHELL_CLASSES[@]}"; do
    key="log4j.logger.$class"
    if ! has_config "$key" "$LOG_CONFIG"; then
      echo "$key=\${shell.log.level}" >> "$LOG_CONFIG"
    fi
  done
}
