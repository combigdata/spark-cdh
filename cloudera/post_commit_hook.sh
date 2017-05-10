#!/bin/bash
#
# This script (post_commit_hook.sh) is executed by post-commit jobs
#
# This script is called from inside the spark source code directory, and it
# is used to build and test the current Spark code.
#

# -e will make the script exit if an error happens on any command executed
set -ex

# Script created by Cloudcat with useful environment information
[ -f /opt/toolchain/toolchain.sh ] && . /opt/toolchain/toolchain.sh

# Use JAVA7_HOME if exists
export JAVA_HOME=${JAVA7_HOME:-$JAVA_HOME}

# If JDK_VERSION exists, then try to get the value from JAVAX_HOME
if [ -n "$JDK_VERSION" ]; then
  # Get JAVAX_HOME value, where X is the JDK version
  java_home=`eval echo \\$JAVA${JDK_VERSION}_HOME`
  if [ -n "$java_home" ]; then
    export JAVA_HOME="$java_home"
  else
    echo "ERROR: USE_JDK_VERSION=$JDK_VERSION, but JAVA${JDK_VERSION}_HOME is not found."
    exit 1
  fi
fi

export PATH=${JAVA_HOME}/bin:${PATH}

# To make some of the output quieter
export AMPLAB_JENKINS=1

MVN_REPO_LOCAL=$HOME/.m2/repository${M2_REPO_SUFFIX}

export MAVEN_OPTS="-XX:PermSize=1024m -XX:MaxPermSize=1024m -XX:ReservedCodeCacheSize=512m"

export APACHE_MIRROR=http://mirror.infra.cloudera.com/apache
./build/mvn --force -B -Dcdh.build=true -P-hive package -fae -Dmaven.repo.local="$MVN_REPO_LOCAL"
