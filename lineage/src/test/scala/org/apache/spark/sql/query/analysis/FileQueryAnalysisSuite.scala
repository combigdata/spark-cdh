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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.query.analysis.DataSourceType.DataSourceType
import org.apache.spark.sql.query.analysis.TestUtils._
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Tests that reading and writing to the local and HDFS file systems produces the desired lineage.
 */
class FileQueryAnalysisSuite extends ParquetTest with ParquetHDFSTest with TestHiveSingleton
 {

  test("Local file works") {
    testSimpleQuery(withParquetFile, DataSourceType.LOCAL)
  }

  test("Multiple local files works") {
    testMultipleFiles(withParquetFile, DataSourceType.LOCAL)
  }

  test("HDFS file works") {
    testSimpleQuery(withParquetHDFSFile, DataSourceType.HDFS)
  }

  test("Multiple HDFS files works") {
    testMultipleFiles(withParquetHDFSFile, DataSourceType.HDFS)
  }

  def testSimpleQuery(
      fileFunc: (Seq[Customer]) => (String => Unit) => Unit,
      dataSourceType: DataSourceType): Unit = {
    sqlContext.listenerManager.register(TestQeListener)
    fileFunc((1 to 4).map(i => Customer(i, i.toString))) { parquetFile =>
      val df = sqlContext.read.load(parquetFile).select("id", "name")
      df.write.save(parquetFile + "_output")
      val (qe, extraParams) = TestQeListener.getAndClear()
      val inputMetadata = QueryAnalysis.getInputMetadata(qe)
      assert(inputMetadata.length === 2)
      assertHDFSFieldExists(inputMetadata, Array(parquetFile), "id", dataSourceType)
      assertHDFSFieldExists(inputMetadata, Array(parquetFile), "name", dataSourceType)
      val outputMetadata = QueryAnalysis.getOutputMetaData(df.queryExecution, extraParams)
      assert(outputMetadata.isDefined)
      assert(outputMetadata.get.fields.forall(Seq("id", "name").contains(_)))
      assert(outputMetadata.get.dataSourceType === dataSourceType)
      assert(outputMetadata.get.source === getScheme(dataSourceType) + parquetFile + "_output")
    }
  }

  def testMultipleFiles(
      fileFunc: (Seq[Customer]) => (String => Unit) => Unit,
      dataSourceType: DataSourceType): Unit = {
    sqlContext.listenerManager.register(TestQeListener)
    fileFunc((1 to 4).map(i => Customer(i, i.toString))) { parquetFile: String =>
      fileFunc((1 to 4).map(i => Customer(i, i.toString))) { parquetFile2: String =>
        fileFunc((1 to 4).map(i => Customer(i, i.toString))) { parquetFile3 =>
          val parquetFiles = Array(parquetFile, parquetFile2, parquetFile3)
          val df = sqlContext.read.load(parquetFiles: _*).select("id", "name")
          df.write.save(parquetFile + "_output")
          val (qe, extraParams) = TestQeListener.getAndClear()
          val inputMetadata = QueryAnalysis.getInputMetadata(qe)
          assert(inputMetadata.length === 2)
          assertHDFSFieldExists(inputMetadata, parquetFiles, "id", dataSourceType)
          assertHDFSFieldExists(inputMetadata, parquetFiles, "name", dataSourceType)
          val outputMetadata = QueryAnalysis.getOutputMetaData(df.queryExecution, extraParams)
          assert(outputMetadata.isDefined)
          assert(outputMetadata.get.fields.forall(Seq("id", "name").contains(_)))
          assert(outputMetadata.get.dataSourceType === dataSourceType)
          assert(outputMetadata.get.source === getScheme(dataSourceType) + parquetFile + "_output")
        }
      }
    }
  }
}
