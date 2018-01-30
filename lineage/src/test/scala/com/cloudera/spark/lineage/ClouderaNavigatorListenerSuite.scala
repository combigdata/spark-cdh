/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.lineage

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import scala.collection.JavaConverters._

// scalastyle:off
import org.scalatest.FunSuite

import org.apache.spark.SparkConf

class ClouderaNavigatorListenerSuite extends FunSuite {
// scalastyle:on

  import ClouderaNavigatorListener._

  test("lineage file created with correct permissions") {
    val appId = "test_app_id"
    val tmpDir = Files.createTempDirectory("lineage-").toFile()
    val conf = new SparkConf().set(SPARK_LINEAGE_DIR_PROPERTY, tmpDir.getAbsolutePath())

    val elem = new LineageElement()
    elem.applicationID = appId
    elem.timestamp = System.currentTimeMillis()
    elem.duration = 42
    elem.user = "bob"

    writeToLineageFile(elem, appId, conf)
    assert(tmpDir.listFiles().length === 1)

    val file = outputFile(appId).get
    assert(file.toFile().length() > 0)

    val perms = Files.getPosixFilePermissions(file).asScala
    assert(perms === Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))

    untrackOutputFile(appId)
    assert(outputFile(appId) === None)
  }

}
