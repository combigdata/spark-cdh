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

import java.util.HashMap

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.query.analysis.{DataSourceFormat, DataSourceType, QueryDetails}

import scala.collection.mutable.ListBuffer
// scalastyle:off
import org.scalatest.FunSuite

class LineageElementSuite extends FunSuite {
  // scalastyle:on

  val hiveMetaStoreLocation: String = "thrift://localhost:8090/hive_location"

  test("Test the LineageElement is serialzied and deserialized into json properly") {
    val mySparkApp = "MySparkApp"
    val myExecutionId: String = "MyExecutionID"
    val timestamp: Long = 1234567890
    val duration: Long = 1000
    val user: String = "Myself"
    val inputTable: String = "MyTable"
    val col1: String = "MyCol1"
    val col2: String = "MyCol2"
    val inputTable2: String = "MyTable2"
    val col3: String = "MyCol12"
    val col4: String = "MyCol22"
    val outputFile: String = "MyOutputFile"
    val outputCol1: String = "MyOutputCol1"
    val outputCol2: String = "MyOutputCol2"

    val lineageElement = new LineageElement()
    lineageElement.applicationID = mySparkApp
    lineageElement.timestamp = timestamp
    lineageElement.duration = duration
    lineageElement.user = user
    val qd1 = new QueryDetails(inputTable, List(col1, col2).to[ListBuffer],
      DataSourceType.HIVE, DataSourceFormat.PARQUET)
    qd1.hiveMetastoreLocation = Some(hiveMetaStoreLocation)
    lineageElement.addInput(qd1)
    val qd2 = new QueryDetails(inputTable2, List(col3, col4).to[ListBuffer],
      DataSourceType.HIVE, DataSourceFormat.AVRO)
    qd2.hiveMetastoreLocation = Some(hiveMetaStoreLocation)
    lineageElement.addInput(qd2)
    lineageElement
      .addOutput(new QueryDetails(outputFile, List(outputCol1, outputCol2).to[ListBuffer],
          DataSourceType.HDFS, DataSourceFormat.JSON))

    val mapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .setSerializationInclusion(Include.NON_NULL)
      .setSerializationInclusion(Include.NON_EMPTY)
    val map =
      mapper.readValue(mapper.writeValueAsString(lineageElement), classOf[HashMap[String, Any]])
    assert(map.get("applicationID") === mySparkApp)
    assert(map.get("timestamp") === timestamp)
    assert(map.get("user") === user)
    assert(map.get("duration") === duration)
    assert(map.get("ended") === false)

    val inputList: List[Map[String, Any]] = map.get("inputs").asInstanceOf[List[Map[String, Any]]]
    assert(inputList.size === 2)
    assertMapElements(inputList(0), inputTable, Seq(col1, col2), "HIVE", "PARQUET")
    assertMapElements(inputList(1), inputTable2, Seq(col3, col4), "HIVE", "AVRO")
    val outputList: List[Map[String, Any]] = map.get("outputs").asInstanceOf[List[Map[String, Any]]]
    assertMapElements(outputList(0), outputFile, Seq(outputCol1, outputCol2), "HDFS", "JSON")
  }

  private def assertMapElements(
      elem: Map[String, Any],
      source: String,
      cols: Seq[String],
      dataSourceType: String,
      dataSourceFormat: String) {
    assert(elem.get("source").get === source)
    val fields: List[String] = elem.get("fields").get.asInstanceOf[List[String]]
    assert(fields.size === 2)
    assert(fields.forall(cols.contains(_)))
    assert(elem.get("dataSourceType").get === dataSourceType)
    assert(elem.get("dataSourceFormat").get === dataSourceFormat)
    if(dataSourceType == "HIVE") {
      assert(elem.get("hiveMetastoreLocation").get === hiveMetaStoreLocation)
    } else {
      assert(!elem.get("hiveMetaStoreLocation").isDefined)
    }
  }
}
