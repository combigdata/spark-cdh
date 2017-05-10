#!/bin/bash -e
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

if ! git status | grep -q 'nothing to commit'; then
  echo "Source directory is not clean, aborting."
  exit 1
fi

SPARK_HOME="$(cd "$(dirname "$0")"/..; pwd)"
VERSION=$(cd $SPARK_HOME; build/mvn help:evaluate -Dexpression=project.version \
          -Dcdh.build=true -pl :spark-parent_2.11 2>/dev/null | \
          grep -v "INFO" | tail -n 1)

# If the current version is -SNAPSHOT, then we're preparing for release. Otherwise, we're
# preparing for development of the next version, so bump the cloudera revision and add
# -SNAPSHOT back.
if [[ $VERSION =~ .+-SNAPSHOT ]]; then
  NEW_VERSION="${VERSION/-SNAPSHOT/}"
  COMMIT_MESSAGE="CLOUDERA-BUILD. Preparing for release $NEW_VERSION."
  UPDATE_CSD=0
else
  NEW_VERSION="${VERSION}-SNAPSHOT"
  SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-3)
  REV=$(echo "$VERSION" | cut -d . -f 4 | awk -Fcloudera '{print $2}')
  REV=$((REV + 1))
  NEW_VERSION="${SHORT_VERSION}.cloudera${REV}-SNAPSHOT"
  COMMIT_MESSAGE="CLOUDERA-BUILD. Preparing development version $NEW_VERSION."
  UPDATE_CSD=1
fi

echo "Current version is $VERSION"
echo "Next version is $NEW_VERSION"

if [[ $UPDATE_CSD = 1 ]]; then
  CSD_DESCRIPTOR="$SPARK_HOME/csd/src/descriptor/service.sdl"
  CSD_VERSION=$(grep '"version" :' "$CSD_DESCRIPTOR" | head -n 1 | awk '-F"' '{ print $4 }')
  TMP=$(mktemp)
  NEW_CSD_VERSION=$(echo "$NEW_VERSION" | cut -d - -f 1)

  echo "CSD version is $NEW_VERSION"
  echo "Next CSD version is $NEW_CSD_VERSION"
  sed -e "s/$CSD_VERSION/$NEW_CSD_VERSION/" "$CSD_DESCRIPTOR" > "$TMP"
  mv "$TMP" "$CSD_DESCRIPTOR"
fi

mvn -B -q versions:set -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false 1>/dev/null 2>&1

git commit -a -m "$COMMIT_MESSAGE"
git show
