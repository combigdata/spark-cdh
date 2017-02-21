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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.query.analysis.TestUtils._

/**
 * Tests that check that reading and writing to Hive tables produce the desired lineage data
 */
class HiveQueryAnalysisSuite
    extends SparkFunSuite
    with TestHiveSingleton
    with SQLTestUtils
    with ParquetHDFSTest {

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
    val df = hiveContext.sql("select code, description, salary from test_table_1")
    assertHiveInputs(df.queryExecution, "test_table_1", Seq("code", "description", "salary"))
  }

  test("QueryAnalysis.getInputMetadata return back InputMetadata for complex joins") {
    var df2 = hiveContext.sql(
        "select code, sal from (select o.code as code,c.description as desc," +
          "c.salary as sal from test_table_1 c join test_table_2 o on (c.code = o.code)"
          + " where c.salary > 170000 sort by sal)t1 limit 3")
    df2 = df2.filter(df2("sal") > 100000)
    df2.write.saveAsTable("mytable")
    val qe = TestQeListener.getAndClear()
    val inputMetadata = QueryAnalysis.getInputMetadata(qe)
    assert(inputMetadata.length === 2)
    assertHiveFieldExists(inputMetadata, "test_table_1", "salary")
    assertHiveFieldExists(inputMetadata, "test_table_2", "code")
    assertHiveOutputs(qe, "mytable", Seq("code", "sal"))
  }

  test("QueryAnalysis.getInputMetadata returns back InputMetadata for * queries") {
    val df = hiveContext.sql("select * from test_table_1")
    assertHiveInputs(df.queryExecution, "test_table_1", Seq("code", "description", "salary",
      "total_emp"))
  }

  test("There is fully qualified table name in OutputMetadata") {
    val df = hiveContext.sql("select * from test_table_1")
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.saveAsTable("mytable")
        val qe = TestQeListener.getAndClear()
        assertHiveInputs(qe, "test_table_1", Seq("total_emp", "salary", "description", "code"))
        assertHiveOutputs(qe, "mytable", Seq("total_emp", "salary", "description", "code"), db)
      }
    }
  }

  test("CDH-50079 : a hive table joined with a parquet temp table is listed correctly") {
    withParquetHDFSFile((1 to 4).map(i => Customer(i, i.toString))) { prq =>
      sqlContext.read.parquet(prq).registerTempTable("customers")
      sqlContext
        .sql("select test_table_1.code, customers.name from test_table_1 join customers where " +
          "test_table_1.code = customers.id and test_table_1.description = 'Tom Cruise'")
        .write.saveAsTable("myowntable")
      val qe = TestQeListener.getAndClear()
      val inputMetadata = QueryAnalysis.getInputMetadata(qe)
      assert(inputMetadata.length === 2)
      assertHiveFieldExists(inputMetadata, "test_table_1", "code")
      assertHDFSFieldExists(inputMetadata, Array(prq), "name", DataSourceType.HDFS)
      val outputMetadata = QueryAnalysis.getOutputMetaData(qe)
      assert(outputMetadata.isDefined)
      assert(outputMetadata.get.source === "default.myowntable")
      assert(outputMetadata.get.dataSourceType === DataSourceType.HIVE)
    }
  }

  test("CDH-50366: Lineage should output data when there is no inputs") {
    import hiveContext.implicits._
    val nonInputTable: String = "MyNonInputTable"

    val nonInputDF = (1 to 4).map { i =>
      (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
    }.toDF
    nonInputDF.write.saveAsTable(nonInputTable)
    var qe = TestQeListener.getAndClear()
    assert(QueryAnalysis.getInputMetadata(qe).length === 0)
    assertHiveOutputs(qe, nonInputTable, Seq( "_1", "_2", "_3", "_4", "_5"))
    withTempPath { f =>
      nonInputDF.write.json(f.getCanonicalPath)
      val qe = TestQeListener.getAndClear()
      assert(QueryAnalysis.getInputMetadata(qe).length === 0)
      val outputMetadata = QueryAnalysis.getOutputMetaData(qe)
      assert(outputMetadata.isDefined)
      assert(outputMetadata.get.fields.size === 5)
      assert(outputMetadata.get.source === "file:" + f.getCanonicalPath)
      assert(outputMetadata.get.dataSourceType === DataSourceType.LOCAL)
    }

    var anotherDF = hiveContext.sql("select code, description, salary from test_table_1")
    anotherDF = anotherDF.join(nonInputDF, nonInputDF.col("_3") === anotherDF.col("salary"))
    anotherDF.write.saveAsTable(nonInputTable + 2)
    qe = TestQeListener.getAndClear()
    assertHiveInputs(qe, "test_table_1", Seq("salary", "code", "description"))
    assertHiveOutputs(qe, nonInputTable + 2, Seq("code", "description", "salary", "_1", "_2", "_3",
      "_4", "_5"))

    anotherDF.select("_2", "_5", "salary", "code").write.saveAsTable(nonInputTable + 3)
    qe = TestQeListener.getAndClear()
    assertHiveInputs(qe, "test_table_1", Seq("salary", "code"))
    assertHiveOutputs(qe, nonInputTable + 3, Seq("code", "salary", "_2", "_5"))

    var personDF = (1 to 4).map(i => Person(i.toString, i)).toDF
    personDF.write.saveAsTable(nonInputTable + 4)
    qe = TestQeListener.getAndClear()
    val inputMetadata = QueryAnalysis.getInputMetadata(qe)
    assert(inputMetadata.length === 0)
    assertHiveOutputs(qe, nonInputTable + 4, Seq("name", "age"))

    personDF = personDF.join(nonInputDF, nonInputDF.col("_3") === personDF.col("age"))
    personDF.write.saveAsTable(nonInputTable + 5)
    qe = TestQeListener.getAndClear()
    assert(QueryAnalysis.getInputMetadata(qe).length === 0)
    assertHiveOutputs(qe, nonInputTable + 5, Seq("name", "age" , "_1", "_2", "_3",
      "_4", "_5"))

    val testTableDF = hiveContext.sql("select code, description, salary from test_table_1")
    personDF = (1 to 4).map(i => Person(i.toString, i)).toDF
    personDF = personDF.join(testTableDF, testTableDF.col("salary") === personDF.col("age"))
    personDF.select("code", "description", "name", "age").write.saveAsTable(nonInputTable + 6)
    qe = TestQeListener.getAndClear()
    assertHiveInputs(qe, "test_table_1", Seq("description", "code"))
    assertHiveOutputs(qe, nonInputTable + 6, Seq("code", "description", "name", "age"))
  }

  def assertHiveInputs(
      qe: org.apache.spark.sql.execution.QueryExecution,
      table: String,
      columns: Seq[String],
      db: String = "default") {
    val inputMetadata = QueryAnalysis.getInputMetadata(qe)
    assert(inputMetadata.length === columns.size)
    columns.foreach(assertHiveFieldExists(inputMetadata, table, _, db))
  }

  def assertHiveOutputs(
      qe: org.apache.spark.sql.execution.QueryExecution,
      table: String,
      columns: Seq[String],
      db: String = "default") {
    val outputMetadata = QueryAnalysis.getOutputMetaData(qe)
    assert(outputMetadata.isDefined)
    assert(outputMetadata.get.fields.size === columns.size)
    assert(outputMetadata.get.source === db + "." + table)
    assert(outputMetadata.get.dataSourceType === DataSourceType.HIVE)
    assert(outputMetadata.get.fields.forall(columns.contains(_)))
  }

  implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = { () =>
      new QueryExecution(sql).stringResult(): Unit
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    hiveContext.listenerManager.clear()
  }
}

case class Person(name: String, age: Long)
