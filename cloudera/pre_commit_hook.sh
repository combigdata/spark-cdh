#!/bin/bash
#
# This script (pre_commit_hook.sh) is executed by pre-commit jobs
#
# This script is called from inside the spark source code directory, and it
# is used to build and test the current Spark code.
#

# -e will make the script exit if an error happens on any command executed
set -ex

export PATH=${JAVA_HOME}/bin:${PATH}

# To make some of the output quieter
export AMPLAB_JENKINS=1

MVN_REPO_LOCAL=$HOME/.m2/repository${M2_REPO_SUFFIX}

export MAVEN_OPTS="-XX:ReservedCodeCacheSize=512m"

export APACHE_MIRROR=http://mirror.infra.cloudera.com/apache

# install mvn settings so dependencies come from a GBN of the latest build
export CDH_GBN="$(curl "http://builddb.infra.cloudera.com:8080/query?product=cdh&version=6.x&user=jenkins&tag=official")"
MVN_SETTINGS_FILE="$(mktemp)"
function cleanup {
  rm -f "$MVN_SETTINGS_FILE"
}
trap cleanup EXIT
curl -L "http://github.mtv.cloudera.com/raw/CDH/cdh/cdh6.x/gbn-m2-settings.xml" > "$MVN_SETTINGS_FILE"

./build/mvn -U -s "$MVN_SETTINGS_FILE" -B -Dcdh.build=true package -fae -Dmaven.repo.local="$MVN_REPO_LOCAL"
