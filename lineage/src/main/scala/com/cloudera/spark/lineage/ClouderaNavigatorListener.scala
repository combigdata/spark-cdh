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

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.query.analysis.QueryAnalysis
import org.apache.spark.sql.util.QueryExecutionListener

private class NavigatorAppListener(conf: SparkConf) extends SparkListener {

  LineageWriter.checkLineageConfig(conf)

  private var writer: LineageWriter = _

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    writer = LineageWriter(event.appId.get, event.sparkUser, conf)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    val event = ApplicationEvent(
      writer.lineageId,
      writer.applicationId,
      writer.user,
      System.currentTimeMillis(),
      true)
    writer.write(event, start = false)
    writer.close()
    writer = null
  }

}

private class NavigatorQueryListener(conf: SparkConf) extends QueryExecutionListener {

  LineageWriter.checkLineageConfig(conf)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val writer = LineageWriter(qe.sparkSession.sparkContext.applicationId)
    val info = QueryAnalysis.getLineageInfo(qe)
    if (info.inputs.nonEmpty || info.outputs.nonEmpty) {
      val details = QueryDetails(
        writer.lineageId,
        writer.applicationId,
        writer.user,
        System.currentTimeMillis(),
        info.inputs,
        info.outputs,
        TimeUnit.NANOSECONDS.toMillis(durationNs))

      writer.write(details)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    // Nothing to do.
  }

}
