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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.query.analysis.QueryDetails

/**
 * A class that is used to contain the query execution meta-data that is later on written to
 * the lineage log files
 */
private[lineage] class LineageElement {

  val version = "1.0"
  var applicationID: String = _
  var yarnApplicationId: String = _
  var timestamp: Long = _
  var duration: Long = _
  var user: String = _
  var message: String = _
  var inputs: ListBuffer[QueryDetails] = ListBuffer[QueryDetails]()
  var outputs: ListBuffer[QueryDetails] = ListBuffer[QueryDetails]()
  var ended: Boolean = _

  def addInput(queryDetails: QueryDetails): Unit = {
    inputs += queryDetails
  }

  def addOutput(queryDetails: QueryDetails): Unit = {
    outputs += queryDetails
  }
}
