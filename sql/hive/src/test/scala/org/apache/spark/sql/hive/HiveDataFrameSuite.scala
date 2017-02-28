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

package org.apache.spark.sql.hive


import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.{QueryTest}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.QueryExecutionListener

class HiveDataFrameSuite extends QueryTest with TestHiveSingleton with SQLTestUtils{

  import testImplicits._

  test("table name with schema") {
    // regression test for SPARK-11778
    hiveContext.sql("create schema usrdb")
    hiveContext.sql("create table usrdb.test(c int)")
    hiveContext.read.table("usrdb.test")
    hiveContext.sql("drop table usrdb.test")
    hiveContext.sql("drop schema usrdb")
  }

  test("QueryExecutionListener gets called on DataFrameWriter.saveAsTable method") {
    var onWriteSuccessCalled = false

    hiveContext.listenerManager.register(new QueryExecutionListener {
      override def onFailure( funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess( funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        assert(durationNs > 0)
        assert(qe ne null)
        onWriteSuccessCalled = true
      }
    })
    withTable("bar") {
      Seq(1 -> 100).toDF("x", "y").write.saveAsTable("bar")
    }
    assert(onWriteSuccessCalled)
    sqlContext.listenerManager.clear()
  }
}
