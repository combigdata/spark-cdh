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

import org.apache.spark.sql.query.analysis.ClouderaFunSuite

class LineageElementSuite extends ClouderaFunSuite {

  type JsonMap = Map[String, Any]

  test("serialization matches expected format") {
    val appId = "MySparkApp"
    val executionId = "MyExecutionID"
    val timestamp = 1234567890L
    val duration = 1000L
    val user = "Myself"
    val errorCode = "01"
    val errorMessage = "this is an error"
    val hiveMetaStoreLocation = "thrift://localhost:8090/hive_location"

    val input1 = RelationInfo("table1", Seq("col1", "col2"), DataSourceType.HIVE,
      DataSourceFormat.PARQUET, Some(hiveMetaStoreLocation))
    val input2 = RelationInfo("table2", Seq("col3", "col4"), DataSourceType.HIVE,
      DataSourceFormat.AVRO, Some(hiveMetaStoreLocation))
    val output = RelationInfo("outfile", Seq("out1", "out2"), DataSourceType.HDFS,
        DataSourceFormat.JSON, None)

    // These JSON strings match the Spark lineage specification. Properties should not be
    // changed, added or removed without corresponding changes on the Navigator side.
    val expectedQueryDetails = s"""{
      "version" : "1.0",
      "applicationID" : "$appId",
      "yarnApplicationId" : "$executionId",
      "user" : "$user",
      "timestamp" : $timestamp,
      "duration" : $duration,
      "errorCode" : "$errorCode",
      "message" : "$errorMessage",
      "ended" : false,

      "inputs" : [
        {
          "source" : "${input1.source}",
          "fields" : [ "${input1.fields(0)}", "${input1.fields(1)}" ],
          "dataSourceType" : "${input1.dataSourceType}",
          "dataSourceFormat" : "${input1.dataSourceFormat}",
          "hiveMetastoreLocation" : "$hiveMetaStoreLocation"
        },
        {
          "source" : "${input2.source}",
          "fields" : [ "${input2.fields(0)}", "${input2.fields(1)}" ],
          "dataSourceType" : "${input2.dataSourceType}",
          "dataSourceFormat" : "${input2.dataSourceFormat}",
          "hiveMetastoreLocation" : "$hiveMetaStoreLocation"
        }
      ],

      "outputs" : [
        {
          "source" : "${output.source}",
          "fields" : [ "${output.fields(0)}", "${output.fields(1)}" ],
          "dataSourceType" : "${output.dataSourceType}",
          "dataSourceFormat" : "${output.dataSourceFormat}"
        }
      ]
    }"""

    val expectedApplicationEvent = s"""{
      "version" : "1.0",
      "applicationID" : "$appId",
      "yarnApplicationId" : "$executionId",
      "user" : "$user",
      "timestamp" : $timestamp,
      "ended" : true
    }"""

    val mapper = LineageWriter.createMapper()

    val details = QueryDetails(
      appId,
      executionId,
      user,
      timestamp,
      Seq(input1, input2),
      Seq(output),
      duration,
      errorCode = errorCode,
      message = errorMessage)
    val deser = mapper.readValue(mapper.writeValueAsString(details), classOf[JsonMap])
    val expectedDetails = mapper.readValue(expectedQueryDetails, classOf[JsonMap])
    compareJson(deser, expectedDetails)

    val appEvent = ApplicationEvent(
      appId,
      executionId,
      user,
      timestamp,
      true)
    val deserEvent = mapper.readValue(mapper.writeValueAsString(appEvent), classOf[JsonMap])
    val expectedDeserEvent = mapper.readValue(expectedApplicationEvent, classOf[JsonMap])
    compareJson(deserEvent, expectedDeserEvent)
  }

  /** Compare two json maps. This is mostly to generate a nice, readable error message. */
  private def compareJson(map: JsonMap, exp: JsonMap): Unit = {
    val extra = map.keys.toSet -- exp.keys
    val missing = exp.keys.toSet -- map.keys
    val diff = map.flatMap { case (k, v) if !extra.contains(k) & !missing.contains(k) =>
      if (v != exp(k)) {
        Some(s"Wrong value for $k:\n\tfound   : $v\n\texpected: ${exp(k)}")
      } else {
        None
      }
    }

    val error = new StringBuilder()
    if (extra.nonEmpty) {
      error.append("\nUnexpected keys found in output: ").append(extra.mkString(","))
    }
    if (missing.nonEmpty) {
      error.append("\nMissing keys in the output: ").append(missing.mkString(","))
    }
    diff.foreach { err =>
      error.append("\n").append(err)
    }

    if (error.length() > 0) {
      fail(error.toString())
    }
  }

}
