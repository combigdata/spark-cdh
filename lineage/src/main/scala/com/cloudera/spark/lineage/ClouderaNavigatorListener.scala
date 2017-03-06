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

import java.io.{File, FileNotFoundException, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.query.analysis.{DataSourceType, QueryAnalysis, QueryDetails}
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * The Listener that is responsible for listening to SQL queries and outputting the lineage
 * metadata to the lineage folder.
 */
private[lineage] class ClouderaNavigatorListener
    extends SparkListener
    with QueryExecutionListener
    with Logging {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_EMPTY)
  private val SPARK_LINEAGE_DIR_PROPERTY: String = "spark.lineage.log.dir"
  private val DEFAULT_SPARK_LINEAGE_DIR: String = "/var/log/spark/lineage"

  override def onFailure( funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  override def onSuccess( funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    writeQueryMetadata(qe, durationNs)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val sc = SparkContext.getOrCreate()
    if (checkLineageEnabled(sc)) {
      val lineageElement = getNewLineageElement(sc)
      lineageElement.ended = true
      writeToLineageFile(lineageElement, sc)
    }
  }

  private def writeQueryMetadata( qe: QueryExecution, durationNs: Long): Unit = {
    val sc = SparkContext.getOrCreate()
    if (checkLineageEnabled(sc)) {
      val lineageElement = getNewLineageElement(sc)
      lineageElement.yarnApplicationId = sc.applicationId
      lineageElement.duration = durationNs / 1000000
      lineageElement.user = sc.sparkUser

      val inputMetaData = QueryAnalysis.getInputMetadata(qe)
      QueryAnalysis
        .convertToQueryDetails(inputMetaData)
        .map(addHiveMetastoreLocation(_, qe))
        .foreach(lineageElement.addInput(_))

      QueryAnalysis
        .getOutputMetaData(qe)
        .map(addHiveMetastoreLocation(_, qe))
        .foreach(lineageElement.addOutput(_))

      if (lineageElement.inputs.size > 0 || lineageElement.outputs.size > 0) {
        writeToLineageFile(lineageElement, sc)
      }
    }
  }

  private def getNewLineageElement(sc: SparkContext): LineageElement = {
    val lineageElement = new LineageElement()
    lineageElement.applicationID =
      sc.getConf.getOption("spark.lineage.app.name").getOrElse(sc.applicationId)
    lineageElement.timestamp = System.currentTimeMillis()
    lineageElement
  }

  private def addHiveMetastoreLocation(
      queryDetails: QueryDetails,
      qe: QueryExecution): QueryDetails = {
    if (queryDetails.dataSourceType == DataSourceType.HIVE) {
      queryDetails.hiveMetastoreLocation = Option(new HiveConf().getVar(METASTOREURIS))
    }
    queryDetails
  }

  private def checkLineageEnabled(sc: SparkContext): Boolean = {
    val dir = sc.getConf.get(SPARK_LINEAGE_DIR_PROPERTY, DEFAULT_SPARK_LINEAGE_DIR)
    val enabled = sc.getConf.getBoolean("spark.lineage.enabled", false)
    if (enabled && !Files.exists(Paths.get(dir))) {
      throw new FileNotFoundException(
          s"Lineage is enabled but lineage directory $dir doesn't exist")
    }
    enabled
  }

  /**
   * Write the lineage element to the file, flush and close it so that navigator can see the data
   * immediately
   *
   * @param lineageElement
   * @param sc
   */
  private def writeToLineageFile(lineageElement: LineageElement, sc: SparkContext): Unit =
    synchronized {
      val dir = sc.getConf.get(SPARK_LINEAGE_DIR_PROPERTY, DEFAULT_SPARK_LINEAGE_DIR)
      var fileWriter: OutputStreamWriter = null
      try {
        fileWriter = new OutputStreamWriter(
            new FileOutputStream(dir + File.separator + "spark_lineage_log_" + sc.applicationId
                  + "-" + sc.startTime + ".log", true), StandardCharsets.UTF_8);
        fileWriter.append(mapper.writeValueAsString(lineageElement) + System.lineSeparator())
      } finally {
        if (fileWriter != null) {
          fileWriter.flush()
          fileWriter.close()
        }
      }
    }
}
