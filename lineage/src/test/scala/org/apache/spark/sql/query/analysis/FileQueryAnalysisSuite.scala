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

package org.apache.spark.sql.query.analysis

import com.cloudera.spark.lineage._

/**
 * Tests that reading and writing to the local and HDFS file systems produces the desired lineage.
 */
class FileQueryAnalysisSuite extends BaseLineageSuite {

  test("local file") {
    testSimpleQuery(DataSourceType.LOCAL)
  }

  test("multiple local files") {
    testMultipleFiles(DataSourceType.LOCAL)
  }

  test("hdfs file") {
    testSimpleQuery(DataSourceType.HDFS)
  }

  test("multiple hdfs files") {
    testMultipleFiles(DataSourceType.HDFS)
  }

  def testSimpleQuery(dsType: DataSourceType): Unit = {
    withInputFiles(dsType, 1) { case Seq(input) =>
      val output = input + "_output"
      spark.read.load(input)
        .select("id", "name")
        .write
        .save(output)

      assertLineage(
        relation(output, Seq("id", "name"), dsType, format = DataSourceFormat.PARQUET),
        relation(input, Seq("id", "name"), dsType, format = DataSourceFormat.PARQUET)
      )
    }
  }

  def testMultipleFiles(dsType: DataSourceType): Unit = {
    withInputFiles(dsType, 3) { inputs =>
      val output = newTempPath(dsType).toString()
      spark.read.load(inputs: _*)
        .select("id", "name")
        .write
        .save(output)

      assertLineage(
        relation(output, Seq("id", "name"), dsType, format = DataSourceFormat.PARQUET),
        inputs.map { path =>
          relation(path, Seq("id", "name"), dsType, format = DataSourceFormat.PARQUET)
        }: _*
      )
    }
  }
}
