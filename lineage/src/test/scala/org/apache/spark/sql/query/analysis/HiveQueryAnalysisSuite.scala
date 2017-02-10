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

import java.io.File

import com.cloudera.spark.lineage.{DataSourceFormat, DataSourceType, FieldDetails}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.scalatest.BeforeAndAfterAll

/**
 * Tests that check that reading and writing to Hive tables produce the desired lineage data
 */
class HiveQueryAnalysisSuite
    extends QueryTest
    with TestHiveSingleton
    with SQLTestUtils
    with BeforeAndAfterAll {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    hiveContext.listenerManager.register(TestQeListener)
    val testDataDirectory = "target" + File.separator + "query-analysis" + File.separator
    val testTables = Seq("test_table_1", "test_table_2").map(s => TestTable(s,
      s"""
         |CREATE EXTERNAL TABLE $s (
         |  code STRING,
         |  description STRING,
         |  total_emp INT,
         |  salary INT)
         |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
         |  STORED AS TEXTFILE LOCATION "${new File(testDataDirectory, s).getCanonicalPath}"
      """.stripMargin.cmd))
    testTables.foreach(registerTestTable)
  }

  test("QueryAnalysis.getInputMetadata returns back InputMetadata for simple queries") {
    val df = sqlContext.sql("select code, description, salary from test_table_1")
    val inputMetadata = QueryAnalysis.getInputMetadata(df.queryExecution)
    assert(inputMetadata.length === 3)
    assertHiveFieldExists(inputMetadata, "test_table_1", "code")
    assertHiveFieldExists(inputMetadata, "test_table_1", "description")
    assertHiveFieldExists(inputMetadata, "test_table_1", "salary")
  }

  test("QueryAnalysis.getInputMetadata return back InputMetadata for complex joins") {
    var df2 = sqlContext.sql(
        "select code, sal from (select o.code as code,c.description as desc," +
          "c.salary as sal from test_table_1 c join test_table_2 o on (c.code = o.code)"
          + " where c.salary > 170000 sort by sal)t1 limit 3")
    df2 = df2.filter(df2("sal") > 100000)
    df2.write.saveAsTable("mytable")
    val (qe, extraParams) = TestQeListener.getAndClear()
    val inputMetadata = QueryAnalysis.getInputMetadata(qe)
    assert(inputMetadata.length === 2)
    assertHiveFieldExists(inputMetadata, "test_table_1", "salary")
    assertHiveFieldExists(inputMetadata, "test_table_2", "code")
    val outputMetadata = QueryAnalysis.getOutputMetaData(qe, extraParams)
    assert(outputMetadata.isDefined)
    assert(outputMetadata.get.fields.forall(Seq("code", "sal").contains(_)))
    assert(outputMetadata.get.dataSourceType === DataSourceType.HIVE)
    assert(outputMetadata.get.source === "default.mytable")
  }

  test("QueryAnalysis.getInputMetadata returns back InputMetadata for * queries") {
    val df = sqlContext.sql("select * from test_table_1")
    val inputMetadata = QueryAnalysis.getInputMetadata(df.queryExecution)
    assert(inputMetadata.length === 4)
    assertHiveFieldExists(inputMetadata, "test_table_1", "code")
    assertHiveFieldExists(inputMetadata, "test_table_1", "description")
    assertHiveFieldExists(inputMetadata, "test_table_1", "salary")
    assertHiveFieldExists(inputMetadata, "test_table_1", "total_emp")
  }

  test("There is fully qualified table name in OutputMetadata") {
    val df = hiveContext.sql("select * from test_table_1")
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.saveAsTable("mytable")
        val (qe, extraParams) = TestQeListener.getAndClear()
        val inputMetadata = QueryAnalysis.getInputMetadata(df.queryExecution)
        assert(inputMetadata.length === 4)
        assertHiveFieldExists(inputMetadata, "test_table_1", "code")
        assertHiveFieldExists(inputMetadata, "test_table_1", "description")
        assertHiveFieldExists(inputMetadata, "test_table_1", "salary")
        assertHiveFieldExists(inputMetadata, "test_table_1", "total_emp")
        val outputMetadata = QueryAnalysis.getOutputMetaData(qe, extraParams)
        assert(outputMetadata.isDefined)
        assert(outputMetadata.get.fields.forall(Seq("code", "description", "salary",
              "total_emp").contains(_)))
        assert(outputMetadata.get.dataSourceType === DataSourceType.HIVE)
        assert(outputMetadata.get.source === db + ".mytable")
      }
    }
  }

  test("CDH-50079 : a hive table registered as a temp table is listed correctly") {
    withParquetHDFSFile((1 to 4).map(i => Customer(i, i.toString))) { prq =>
      sqlContext.read.parquet(prq).registerTempTable("customers")
      sqlContext
        .sql("select test_table_1.code, customers.name from test_table_1 join customers where " +
          "test_table_1.code = customers.id and test_table_1.description = 'Tom Cruise'")
        .write.saveAsTable("myowntable")
      val (qe, extraParams) = TestQeListener.getAndClear()
      val inputMetadata = QueryAnalysis.getInputMetadata(qe)
      assert(inputMetadata.length === 2)
      assertHiveFieldExists(inputMetadata, "test_table_1", "code")
      assertHDFSFieldExists(inputMetadata, Array(prq), "name", DataSourceType.HDFS)
      val outputMetadata = QueryAnalysis.getOutputMetaData(qe, extraParams)
      assert(outputMetadata.isDefined)
      assert(outputMetadata.get.source === "default.myowntable")
      assert(outputMetadata.get.dataSourceType === DataSourceType.HIVE)
    }
  }

  implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = { () =>
      new QueryExecution(sql).stringResult(): Unit
    }
  }

  override def afterAll(): Unit = {
    hiveContext.listenerManager.clear()
    hiveContext.sparkContext.stop()
  }
}
