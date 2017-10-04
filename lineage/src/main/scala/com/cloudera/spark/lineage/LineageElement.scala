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

private abstract class LineageElementV1 {

  final val version = "1.0"

}

/**
 * Models the data written to the output file containing details about a query that was
 * run by Spark.
 *
 * This class is defined in the Spark Lineage specification document and should not be
 * changed without corresponding support on the Navigator side.
 */
private case class QueryDetails(
    applicationID: String,
    yarnApplicationId: String,
    user: String,
    timestamp: Long,
    inputs: Seq[RelationInfo],
    outputs: Seq[RelationInfo],
    duration: Long,
    // Defined by Navigator as a two-character string containing a numeric error code.
    errorCode: String = "00",
    message: String = null) extends LineageElementV1 {

  // For backwards compatibility. This value was always written for every query, but the
  // application is always running when queries are processed.
  val ended = false

}

/**
 * Models the data written to the lineage log for application lifecycle events.
 *
 * This class is defined in the Spark Lineage specification document and should not be
 * changed without corresponding support on the Navigator side.
 */
private case class ApplicationEvent(
    applicationID: String,
    yarnApplicationId: String,
    user: String,
    timestamp: Long,
    ended: Boolean) extends LineageElementV1
