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

import java.io.{File, FileNotFoundException}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.util.UUID

import scala.collection.JavaConverters._
import scala.io.Source

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.query.analysis.ClouderaFunSuite

class ClouderaNavigatorListenerSuite extends ClouderaFunSuite with BeforeAndAfter {

  private var tmpDir: File = _

  before {
    tmpDir = Files.createTempDirectory("lineage_writer_test").toFile()
  }

  after {
    JavaUtils.deleteRecursively(tmpDir)
    tmpDir = null
  }

  test("listener writes lineage info") {
    val count = 10

    withSession(true, tmpDir) { spark =>
      (1 to count).foreach { _ =>
        spark.range(10).write.saveAsTable(tableName())
      }
    }

    val files = tmpDir.listFiles()
    assert(files.length === 1)

    val perms = Files.getPosixFilePermissions(files(0).toPath()).asScala
    assert(perms === Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))

    val log = tmpDir.listFiles()(0)
    val lines = Source.fromFile(log, "UTF-8").getLines().toList
    assert(lines.size === count + 1)

    // Verify that all the lines are valid JSON objects. The test doesn't do an extensive
    // check of the contents since that's done by other suites, and also because some of
    // the data is non-deterministic (e.g. timestamps).
    val mapper = LineageWriter.createMapper()

    lines.take(count).foreach { line =>
      val details = mapper.readValue(line, classOf[QueryDetails])
      assert(details.outputs.size === 1)
    }

    val appEnd = mapper.readValue(lines.last, classOf[ApplicationEvent])
    assert(appEnd.ended)
  }

  test("listener does not write lineage info when disabled") {
    withSession(false, tmpDir) { spark =>
      spark.range(10).write.saveAsTable(tableName())
    }

    assert(tmpDir.listFiles().length === 0)
  }

  test("listener does not write lineage info when no queries are executed") {
    withSession(true, tmpDir) { spark =>
      // Do nothing.
    }

    assert(tmpDir.listFiles().length === 0)
  }

  test("listener throws error if lineage directory does not exist") {
    val doesNotExist = new File(tmpDir, "doesNotExist")
    val ex = intercept[SparkException] {
      withSession(true, doesNotExist) { _ =>
        fail("Should have failed.")
      }
    }

    assert(ex.getCause().isInstanceOf[FileNotFoundException])
  }

  private def withSession(lineage: Boolean, dir: File)(fn: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .config(SparkLauncher.SPARK_MASTER, "local")
      .config("spark.lineage.enabled", lineage.toString)
      .config(LineageWriter.SPARK_LINEAGE_DIR_PROPERTY, dir.getAbsolutePath())
      .config("spark.extraListeners", classOf[NavigatorAppListener].getName())
      .config("spark.sql.queryExecutionListeners", classOf[NavigatorQueryListener].getName())
      .getOrCreate()

    try {
      fn(spark)
    } finally {
      spark.stop()
    }
  }

  private def tableName(): String = {
    UUID.randomUUID().toString().replace("-", "_")
  }

}
