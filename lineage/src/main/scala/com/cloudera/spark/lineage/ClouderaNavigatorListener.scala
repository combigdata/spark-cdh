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

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.mutable.HashMap

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import org.apache.spark.{Logging, SparkConf, SparkContext}
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

  import ClouderaNavigatorListener._

  override def onFailure( funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  override def onSuccess( funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    writeQueryMetadata(qe, durationNs)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val sc = SparkContext.getOrCreate()
    if (checkLineageEnabled(sc)) {
      val lineageElement = getNewLineageElement(sc)
      lineageElement.ended = true
      writeToLineageFile(lineageElement, sc.applicationId, sc.getConf)
    }
    untrackOutputFile(sc.applicationId)
  }

  private def writeQueryMetadata( qe: QueryExecution, durationNs: Long): Unit = {
    val sc = SparkContext.getOrCreate()
    if (checkLineageEnabled(sc)) {
      val lineageElement = getNewLineageElement(sc)
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

      if (QueryAnalysis.hasAggregateFunction(qe.optimizedPlan)) {
        lineageElement.errorCode = "01"
        logInfo(s"Lineage Error code: ${lineageElement.errorCode}. Query Plan has an " +
          s"aggregate clause and lineage for such queries isn't supported:\n" +
          s"${qe.optimizedPlan.toString}")
      }

      if (lineageElement.inputs.size > 0 || lineageElement.outputs.size > 0 ||
          !lineageElement.errorCode.equals("00")) {
        writeToLineageFile(lineageElement, sc.applicationId, sc.getConf)
      }
    }
  }

  private def getNewLineageElement(sc: SparkContext): LineageElement = {
    val lineageElement = new LineageElement()
    lineageElement.applicationID =
      sc.getConf.getOption("spark.lineage.app.name").getOrElse(sc.applicationId)
    lineageElement.timestamp = System.currentTimeMillis()
    lineageElement.yarnApplicationId = sc.applicationId
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

}

private object ClouderaNavigatorListener {

  val SPARK_LINEAGE_DIR_PROPERTY = "spark.lineage.log.dir"
  val DEFAULT_SPARK_LINEAGE_DIR = "/var/log/spark/lineage"

  private val NEW_LINE = System.lineSeparator().getBytes(UTF_8)
  private val MAPPER = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_EMPTY)
    .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)

  private val outputFiles = new HashMap[String, Path]()

  /**
   * Write the lineage element to the file, flush and close it so that navigator can see the data
   * immediately
   *
   * @param lineageElement
   * @param sc
   */
  def writeToLineageFile(lineageElement: LineageElement, appId: String, conf: SparkConf): Unit = {
    synchronized {
      val file = outputFiles.getOrElseUpdate(appId, {
        val dir = Paths.get(conf.get(SPARK_LINEAGE_DIR_PROPERTY, DEFAULT_SPARK_LINEAGE_DIR))
        Files.createTempFile(dir, s"spark_lineage_log_$appId-", ".log")
      })

      val out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
      try {
        MAPPER.writeValue(out, lineageElement)
        out.write(NEW_LINE)
        out.flush()
      } finally {
        out.close()
      }
    }
  }

  def untrackOutputFile(appId: String): Unit = synchronized {
    outputFiles.remove(appId)
  }

  // Visible for testing.
  def outputFile(appId: String): Option[Path] = synchronized {
    outputFiles.get(appId)
  }

}
