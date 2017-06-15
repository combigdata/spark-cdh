#!/bin/bash -ex
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

# We shouldn't be skipping the build by default
SKIP_BUILD=false
RUN_TESTS=false
# To make some of the output quieter
export AMPLAB_JENKINS=1
SPARK_HOME="$(cd "$(dirname "$0")"/..; pwd)"
# Let's parse out the pom file to get the version of Spark2 being built
VERSION=$(cd $SPARK_HOME; build/mvn help:evaluate -Dexpression=project.version \
          -Dcdh.build=true -pl :spark-parent_2.11 2>/dev/null | \
          grep -v "INFO" | tail -n 1)

if [[ "$VERSION" != 2* ]]; then
  echo "Detected version (version=$VERSION) does not start with 2"
  exit 1
fi

# Generate a short version from the POM version, so that we can tag the build so cdep can always
# pick up the latest build from a branch without needing code changes.
SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-3)

# Commitish from github.mtv.cloudera.com/CDH/cdh.git repo
# Taken from a point in time from cdh6.x branch of cdh.git, so someone doesn't pull the rug from underneath us
# If specifying a branch here for testing, specify origin/<branch name>
CDH_GIT_HASH=${CDH_GIT_HASH:-97600e529415428054edb09e210ef01c1ba5760e}

# Commitish from github.mtv.cloudera.com/Starship/cmf.git repo
# Taken from a point in time from master branch of cmf.git, so so someone doesn't pull the rug from underneath us
CMF_GIT_HASH=${CMF_GIT_HASH:-2fd32459c94540e725b99ea5d6ea467fea529e79}

# Directory where the massaged output of Spark build goes
# Massaging here refers to addition of wrappers,
# creating empty configs, etc.
BUILD_OUTPUT_DIR=$SPARK_HOME/dist/build_output
# Directory where final repo with parcels and packages will exist
REPO_OUTPUT_DIR=$SPARK_HOME/dist/repo_output
REPO_NAME=spark2-repo
VERSION_FOR_BUILD=${VERSION/-SNAPSHOT/}
# Directory where cdh.git will get cloned
# TODO: Fix this if CDH_GIT_HASH contains a slash.
CDH_CLONE_DIR=${SPARK_HOME}/build/cdh-${CDH_GIT_HASH}

MAKE_MANIFEST_LOCATION=http://github.mtv.cloudera.com/raw/Starship/cmf/${CMF_GIT_HASH}/cli/make_manifest/make_manifest.py

# We are building a non-patch build by default
PATCH_NUMBER=0

BUILDDB_HOST=${BUILDDB_HOST:-"builddb.infra.cloudera.com:8080"}
STORAGE_HOST=${STORAGE_HOST:-"console.aws.amazon.com/s3/home?region=us-west-1#&bucket=cloudera-build&prefix=build"}

CSD_WILDCARD="$SPARK_HOME/csd/target/SPARK2_ON_YARN*.jar"
PYTHON_VE=$(mktemp -d /tmp/__spark2_ve.XXXXX)

GBN=

# Days after the build will expire if being published
EXPIRE_DAYS=${EXPIRE_DAYS:-10}

function usage {
  set +x
  cat <<EOF
build.sh for building a parcel from source code. Can be called from any working directory.
Requirements: JAVA_HOME needs to be set, Python 2.7 or higher
Call simply as build.sh

Options:
 -h or --help:  print usage
 -s or --skip-build:  skip the build. The bits built from last time used to build the parcel.
 -p <patch number> or --patch-num <patch number>:  when building a patch
 --publish: for publishing to S3
 --build-only: for only doing the build (i.e. only building distribution tar.gz, no parcel etc.)
 -t or --with-tests: run unit tests after the build (and optional publishing) is complete.
 --os <osname>: choose the os that the parcel should be built for. The OS name should be the long
                name (like Redhat6). For each os a seperate --os should be used. If the --os is
                not provided, the parcel will be built for all the supported distributions.
EOF
}

function my_echo {
  echo "BUILD_SCRIPT: $1"
}

function clean {
  rm -rf ${BUILD_OUTPUT_DIR}
  # TODO: You are going to lose your previously build parcels, even if this
  # build fails to generate new parcels. Ugh, sorry, but may be we will fix
  # it later
  rm -rf ${REPO_OUTPUT_DIR}
}

function setup {
# Let's get the Global Build Number before we do anything else
  GBN=$(curl http://gbn.infra.cloudera.com/)
  if [[ -z "$GBN" ]]; then
    >&2 my_echo "Unable to retrieve Global Build Number. Are you sure you are on VPN?"
    exit 1
  fi
  if [[ ! -d $CDH_CLONE_DIR ]]; then
    git clone git://github.mtv.cloudera.com/CDH/cdh.git $CDH_CLONE_DIR
  fi
  (cd $CDH_CLONE_DIR; git fetch; git checkout $CDH_GIT_HASH)
  virtualenv $PYTHON_VE
  source $PYTHON_VE/bin/activate
  REQUIREMENTS=$CDH_CLONE_DIR/lib/python/cauldron/requirements.txt
  SETUP_PY=$CDH_CLONE_DIR/lib/python/cauldron/setup.py
  $PYTHON_VE/bin/pip install -r $REQUIREMENTS
  (cd $(dirname $SETUP_PY) && $PYTHON_VE/bin/python $SETUP_PY install)
}

# Runs the make-distribution command, generates bits under SPARK_HOME/dist
function do_build {
  # We want to cd to SPARK_HOME when calling this function. So, let's start a subshell
  # and cd, so in case it errors we are back in the user's original cwd.
  (
  cd $SPARK_HOME
  # On dev boxes, this variable won't be defined, but on Jenkins boxes, it would be
  # set to deploy to also deploy bits to artifactory
  if [[ -z "${DO_MAVEN_DEPLOY}" ]]; then
      MAVEN_INST_DEPLOY=install
  else
     if [[ $PATCH_NUMBER -eq 0 ]]; then
         MAVEN_INST_DEPLOY=$DO_MAVEN_DEPLOY
     else
         my_echo "Cannot deploy with a patch build. Unset DO_MAVEN_DEPLOY"
         exit 1
     fi
  fi

  BUILD_OPTS="-Divy.home=${HOME}/.ivy2 -Dsbt.ivy.home=${HOME}/.ivy2 -Duser.home=${HOME} \
              -Drepo.maven.org=$IVY_MIRROR_PROP \
              -Dreactor.repo=file://${HOME}/.m2/repository${M2_REPO_SUFFIX} \
              -DskipTests -DrecompileMode=all"
  # this might be an issue at times
  # http://maven.40175.n5.nabble.com/Not-finding-artifact-in-local-repo-td3727753.html
  export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m -XX:PermSize=1024m -XX:MaxPermSize=1024m"

  mkdir -p target/zinc
  # This mktemp works with both GNU (Linux) and BSD (Mac) versions
  MYMVN=$(mktemp target/mvn.XXXXXXXX)
  cat >$MYMVN <<EOF
#!/bin/sh
export ZINC_OPTS="-Dzinc.dir=$SPARK_HOME/target/zinc -Xmx2g -XX:MaxPermSize=512M \
                  -XX:ReservedCodeCacheSize=512m"
export APACHE_MIRROR=http://archive-primary.cloudera.com/tarballs/apache
exec $SPARK_HOME/build/mvn --force "\$@"
EOF
  chmod 700 $MYMVN

  my_echo "Building distribution ..."
  ./dev/make-distribution.sh --tgz --mvn $MYMVN --target $MAVEN_INST_DEPLOY \
   -Dcdh.build=true $BUILD_OPTS

  rm -f $MYMVN
  my_echo "Build completed successfully. Distribution at $SPARK_HOME/dist"
  )
}

# Create binary wrappers, etc.
# Picks up the dist generated by build step under SPARK_HOME/dist and add to it.
function post_build_steps {
  my_echo "Creating binary wrappers ..."
  PREFIX=${BUILD_OUTPUT_DIR}

  LIB_DIR=${LIB_DIR:-/usr/lib/spark2}
  INSTALLED_LIB_DIR=${INSTALLED_LIB_DIR:-/usr/lib/spark2}
  BIN_DIR=${BIN_DIR:-/usr/bin}
  CONF_DIR=${CONF_DIR:-/etc/spark2/conf.dist}

  install -d -m 0755 $PREFIX/$LIB_DIR
  install -d -m 0755 $PREFIX/$LIB_DIR/bin
  install -d -m 0755 $PREFIX/$LIB_DIR/sbin
  install -d -m 0755 $PREFIX/$DOC_DIR

  install -d -m 0755 $PREFIX/var/lib/spark/
  install -d -m 0755 $PREFIX/var/log/spark2/
  install -d -m 0755 $PREFIX/var/run/spark2/
  install -d -m 0755 $PREFIX/var/run/spark2/work/

  # Something like $SPARK_HOME/dist
  PARENT_BUILD_OUTPUT_DIR=$(dirname $BUILD_OUTPUT_DIR)
  # Something like build_output
  BASENAME_BUILD_OUTPUT_DIR=$(basename $BUILD_OUTPUT_DIR)
  # Copy of all contents from build_output to the destination, while making sure
  # not to get in a circular loop
  (cd $PARENT_BUILD_OUTPUT_DIR; cp -r $(ls $PARENT_BUILD_OUTPUT_DIR |\
    grep -v $BASENAME_BUILD_OUTPUT_DIR) $PREFIX/$LIB_DIR)

  install -d -m 0755 $PREFIX/$CONF_DIR
  rm -rf $PREFIX/$LIB_DIR/conf
  ln -s /etc/spark2/conf $PREFIX/$LIB_DIR/conf

  # No default /etc/default/spark2 file is shipped because it's only used by
  # services and in case of parcels, CM manages services, so we are fine here.
  # Not shipping spark-env.sh either

  # Create wrappers
  install -d -m 0755 $PREFIX/$BIN_DIR
  for wrap in bin/spark-shell bin/spark-submit; do
  modified_wrap=$(echo ${wrap} | sed -e 's/spark/spark2/g')
  cat > $PREFIX/$BIN_DIR/$(basename $modified_wrap) <<EOF
#!/bin/bash

# Autodetect JAVA_HOME if not defined
. /usr/lib/bigtop-utils/bigtop-detect-javahome

exec $INSTALLED_LIB_DIR/$wrap "\$@"
EOF
    chmod 755 $PREFIX/$BIN_DIR/$(basename $modified_wrap)
  done

  ln -s /var/run/spark2/work $PREFIX/$LIB_DIR/work

  cat > $PREFIX/$BIN_DIR/pyspark2 <<EOF
#!/bin/bash

# Autodetect JAVA_HOME if not defined
. /usr/lib/bigtop-utils/bigtop-detect-javahome

export PYSPARK_PYTHON=\${PYSPARK_PYTHON:-${PYSPARK_PYTHON}}

exec $INSTALLED_LIB_DIR/bin/pyspark "\$@"
EOF
  chmod 755 $PREFIX/$BIN_DIR/pyspark2

  GIT_HASH=$(cd $SPARK_HOME;git rev-parse HEAD)

  install -d -m 0755 $PREFIX/$LIB_DIR/cloudera
  # Generate cdh_version.properties
  cat > $PREFIX/$LIB_DIR/cloudera/spark2_version.properties <<EOF
# Autogenerated build properties
version=$VERSION
git.hash=$GIT_HASH
cloudera.hash=$GIT_HASH
cloudera.cdh.hash=na
cloudera.cdh-packaging.hash=na
cloudera.base-branch=na
cloudera.build-branch=$(git symbolic-ref --short HEAD)
cloudera.pkg.version=na
cloudera.pkg.release=na
cloudera.cdh.release=$VERSION
cloudera.build.time=$(date -u "+%Y.%m.%d-%H:%M:%SGMT")
cloudera.pkg.name=spark2
EOF
}

function build_parcel {
  # The regex is complicated for grep that's the only one that easily worked
  # with default modes on GNU grep and BSD grep (given that we have some mac
  # users on the team)
  CDH_VERSION=$(cd $SPARK_HOME;build/mvn -Dcdh.build=true help:evaluate \
                -Dexpression=hadoop.version |\
                grep "^[0-9]\+\.[0-9]\+\.[0-9]\+-cdh[0-9]\+\.[0-9]\+\.[0-9]\+$" |\
                sed -e 's/.*-cdh\(.*\)/\1/g')
  if [[ -z "$CDH_VERSION" ]]; then
    >&2 my_echo "Unable to find the version of CDH, Spark2 was built against."
    exit 1
  fi

  # util.py needs to exist in $SPARK_HOME/cloudera directory because it's used by
  # a python module (build_parcel.py) from that directory as well. Let's force
  # overwrite to make sure we never the stale version
  (cd $SPARK_HOME/cloudera; rm -f util.py; cp ${CDH_CLONE_DIR}/lib/python/cauldron/src/cauldron/tools/parcel/util.py .)
  CMD="${SPARK_HOME}/cloudera/build_parcel.py --input-directory ${BUILD_OUTPUT_DIR} \
          --output-directory ${OUTPUT_DIR}/parcels --release-version 1 \
          --spark2-version $VERSION --cdh-version $CDH_VERSION --build-number $GBN \
          --patch-number ${PATCH_NUMBER} --verbose --force-clean"

  if [[  ${OS_ARGS_PARCELS} ]]; then
    CMD="$CMD""$OS_ARGS_PARCELS"
  fi
  eval $CMD

  mkdir -p ${OUTPUT_DIR}/csd
  cp ${CSD_WILDCARD} ${OUTPUT_DIR}/csd
}

function populate_manifest {
  # curl -O overwrites, if the file already exists, so we don't have to worry about that
  (cd $SPARK_HOME/target; curl -O $MAKE_MANIFEST_LOCATION)
  chmod 755 $SPARK_HOME/target/make_manifest.py
  $SPARK_HOME/target/make_manifest.py ${OUTPUT_DIR}/parcels
}

function populate_build_json {
  if [[ -z ${OS_ARGS} ]]; then
    for os in $(awk '{print $1}' $SPARK_HOME/cloudera/supported_oses.txt); do
        OS_ARGS=$OS_ARGS" --os $os"
    done
  fi

  CSD_ARGS=""
  for csd in ${CSD_WILDCARD}; do
    CSD_ARGS=$CSD_ARGS" --csd $(basename $csd)"
  done

  if date --date "+$EXPIRE_DAYS days" '+%Y%m%d-%H%M%S' > /dev/null 2>&1; then
    # Linux
    EXPIRY=$(date --date "+$EXPIRE_DAYS days" '+%Y%m%d-%H%M%S')
  else
    # God bless BSD
    EXPIRY=$(date -v "+${EXPIRE_DAYS}d" '+%Y%m%d-%H%M%S')
  fi

  $PYTHON_VE/bin/python ${CDH_CLONE_DIR}/lib/python/cauldron/src/cauldron/tools/buildjson.py \
    -o ${REPO_OUTPUT_DIR}/build.json \
    --product-base spark2:${REPO_NAME} \
    --version $VERSION_FOR_BUILD \
    --parcel-patch-number $PATCH_NUMBER \
    --user $USER \
    --repo ${SPARK_HOME} \
    --gbn $GBN \
    --expiry $EXPIRY \
    $OS_ARGS \
    -s ${CDH_CLONE_DIR}/build-schema.json \
    --build-environment ${HOSTNAME} \
    --product-parcels spark2:${OUTPUT_DIR}/parcels \
    $CSD_ARGS
}

function publish {
  # This file with GBN in it seems to be required by upload.py
  echo ${GBN} > ${REPO_OUTPUT_DIR}/gbn.txt
  $PYTHON_VE/bin/python ${CDH_CLONE_DIR}/lib/python/cauldron/src/cauldron/tools/upload.py s3 ${REPO_OUTPUT_DIR}:${GBN}
  curl http://${BUILDDB_HOST}/save?gbn=${GBN}
  if [[ $PATCH_NUMBER -eq 0 ]]; then
    curl "http://${BUILDDB_HOST}/addtag?gbn=${GBN}&value=${SHORT_VERSION}-latest"
    if [[ "$BUILD_CAUSE" != "TIMERTRIGGER" ]]; then
      curl "http://${BUILDDB_HOST}/addtag?gbn=${GBN}&value=${BUILD_TYPE_TAG}"
    fi
  else
    curl "http://${BUILDDB_HOST}/addtag?gbn=${GBN}&value=released"
  fi
}

function run_tests {
  ${SPARK_HOME}/cloudera/post_commit_hook.sh
  if [[ "$PUBLISH" = true ]]; then
    curl "http://${BUILDDB_HOST}/addtag?gbn=${GBN}&value=unit_tests_passed"
  fi
}

# This is where the main part begins.
# OS_ARGS includes the list of operating systems for buildjson.py and
# OS_ARGS_PARCELS includes the OSes for build_parcel.py
OS_ARGS=""
while [[ $# -ge 1 ]]; do
  arg=$1
  case $arg in
    -p|--patch-num)
    PATCH_NUMBER="$2"
    shift
    ;;
    --os)
    OS_ARGS=$OS_ARGS" --os $2"
    OS_ARGS_PARCELS=$OS_ARGS_PARCELS" --distro $2"
    ;;
    -s|--skip-build)
    SKIP_BUILD=true
    ;;
    --publish)
    PUBLISH=true
    ;;
    --build-only)
    BUILD_ONLY=true
    ;;
    -t|--with-tests)
    RUN_TESTS=true
    ;;
    -h|--help)
    usage
    exit 0
    ;;
    *)
    ;;
  esac
  shift
done

if [[ $PATCH_NUMBER -ne 0 ]]; then
   VERSION_FOR_BUILD=${VERSION/-SNAPSHOT/}_p${PATCH_NUMBER}
fi

# Tag the build with either "snapshot" or "rc", based on the pom version. This allows us to better
# control which tests get automatically executed for each kind of build.
BUILD_TYPE_TAG=snapshot
if ! [[ $VERSION =~ .+-SNAPSHOT ]]; then
  echo "Detected non-snapshot build $VERSION, tagging build as rc."
  BUILD_TYPE_TAG=rc
fi

# Override with a custom version if specified
if [[ -n "$CUSTOM_VERSION" ]]; then
  VERSION_FOR_BUILD=$CUSTOM_VERSION
fi

OUTPUT_DIR=$REPO_OUTPUT_DIR/$REPO_NAME/$VERSION_FOR_BUILD

clean
setup

if [[ "$SKIP_BUILD" = true ]] && [[ "$BUILD_ONLY" = true ]]; then
  my_echo "Can not set --skip-build and --build-only at the same time"
  exit 1
fi

if [[ "$PUBLISH" = true ]] && [[ "$BUILD_ONLY" = true ]]; then
  my_echo "Can not set --publish and --build-only at the same time"
  exit 1
fi

if [[ "$RUN_TESTS" = true ]] && [[ "$BUILD_ONLY" = true ]]; then
  my_echo "Can not set --with-tests and --build-only at the same time"
  exit 1
fi

if [[ "$SKIP_BUILD" = false ]]; then
  do_build
fi

if [[ "$RUN_TESTS" = true ]]; then
  my_echo "Now trying to run unit tests"
  run_tests
  my_echo "Unit tests succeeded."
fi

if [[ "$BUILD_ONLY" != true ]]; then
  post_build_steps
  build_parcel
  populate_manifest
  populate_build_json
  my_echo "Build output: $REPO_OUTPUT_DIR"
fi

if [[ "$PUBLISH" = true ]]; then
  publish
  my_echo "Build published, GBN=$GBN."
  my_echo "Parcels available at the following location:"
  my_echo "https://${STORAGE_HOST}/${GBN}"
fi

my_echo "Build completed. Success!"
