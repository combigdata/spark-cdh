#!/bin/bash
#
# This script (post_commit_hook.sh) is executed by post-commit jobs
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
export SPARK_TESTING=1
if [ -n "$TEST_WITH_JDK_11" ]; then
  # We want to build with JDK_8, and then run all tests w/ JDK_11
  # Just setting JAVA_HOME isn't enough -- scalatests will use whatever java is on the PATH
  export JAVA_HOME=$JAVA_1_8_HOME
  ORIG_PATH=$PATH
  export PATH=$JAVA_HOME/bin:$ORIG_PATH
  ./build/mvn -B -Dcdh.build=true -DskipTests clean install -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS
  echo "Done Building with Java 8, now running tests w/ Java 11"
  export JAVA_HOME=$OPENJDK_11_HOME
  export PATH=$JAVA_HOME/bin:$ORIG_PATH
  echo "Running tests"
  ./build/mvn -B -Dcdh.build=true -Pskip-compilation install -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS
else
  ./build/mvn -B -Dcdh.build=true package -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS
fi
