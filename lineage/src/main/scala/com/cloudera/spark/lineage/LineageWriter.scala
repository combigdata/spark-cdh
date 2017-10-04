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

import java.io.{Closeable, File, FileNotFoundException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Encapsulates the logic to write lineage files. This is kept separate from the listener
 * implementations since multiple listeners may exist in the same process; each Spark application
 * will create at least 2, and a single process may run multiple Spark applications or multiple
 * Spark SQL sessions.
 *
 * The companion object should be used to get instances of this class for each particular app.
 *
 * This class also keeps other state useful for lineage listeners such as application ID and who
 * is running the application.
 */
private class LineageWriter private (
    val applicationId: String,
    val user: String,
    conf: SparkConf)
  extends Closeable with Logging {

  import LineageWriter._

  private val queue = new LinkedBlockingQueue[LineageElementV1]()
  private val started = new AtomicBoolean(false)

  private val worker = new Thread(new Runnable() {
    override def run: Unit = {
      try {
        doWrite()
      } catch {
        case e: Exception =>
          logError("Error in lineage writer thread.", e)
      }
    }
  })

  val lineageId = conf.getOption("spark.lineage.app.name").getOrElse(applicationId)

  worker.setName(s"cloudera-lineage-writer-$applicationId")
  worker.setDaemon(true)

  /**
   * Enqueues an element for writing to the lineage log.
   *
   * @param elem Element to write.
   * @param start Whether to initialize the thread that writes to the log if it hasn't been done
   *              yet.
   */
  def write(elem: LineageElementV1, start: Boolean = true): Unit = {
    queue.put(elem)

    if (start && started.compareAndSet(false, true)) {
      worker.start()
    }
  }

  private def doWrite(): Unit = {
    val mapper = createMapper()
    val dir = Paths.get(conf.get(SPARK_LINEAGE_DIR_PROPERTY, DEFAULT_SPARK_LINEAGE_DIR))
    val file = Files.createTempFile(dir, s"spark_lineage_log_$applicationId-", ".log")
    val out = Files.newOutputStream(file, StandardOpenOption.WRITE)

    try {
      var next = queue.take()
      while (next != POISON_PILL) {
        mapper.writeValue(out, next)
        out.write(NEW_LINE)
        out.flush()
        next = queue.take()
      }
    } finally {
      try {
        out.close()
      } catch {
        case e: Exception =>
          logError(s"Failed to close lineage log.", e)
      }
    }
  }

  override def close(): Unit = {
    try {
      queue.put(POISON_PILL)
      worker.join()
    } finally {
      writers.remove(applicationId)
    }
  }

}

private object LineageWriter {

  private val POISON_PILL = new LineageElementV1() { }
  private val NEW_LINE = System.lineSeparator().getBytes(UTF_8)

  private val writers = new ConcurrentHashMap[String, LineageWriter]()

  val SPARK_LINEAGE_DIR_PROPERTY: String = "spark.lineage.log.dir"
  val DEFAULT_SPARK_LINEAGE_DIR: String = "/var/log/spark/lineage"

  def apply(applicationId: String, user: String, conf: SparkConf): LineageWriter = synchronized {
    Option(writers.get(applicationId)).getOrElse {
      val writer = new LineageWriter(applicationId, user, conf)
      writers.put(applicationId, writer)
      writer
    }
  }

  def apply(applicationId: String): LineageWriter = {
    Option(writers.get(applicationId)).getOrElse(
      throw new IllegalArgumentException(s"No LineageWriter for application $applicationId."))
  }

  /**
   * Check whether lineage is enabled and, if so, whether the configured directory is writable.
   *
   * @throw UnsupportedOperationException If lineage is disabled.
   * @throw FileNotFoundException If configured directory is not usable.
   */
  def checkLineageConfig(conf: SparkConf): Unit = {
    if (!conf.getBoolean("spark.lineage.enabled", false)) {
      throw new UnsupportedOperationException("Lineage is not enabled, disabling listener.")
    }

    val dir = new File(conf.get(SPARK_LINEAGE_DIR_PROPERTY, DEFAULT_SPARK_LINEAGE_DIR))
    if (!dir.isDirectory() || !dir.canWrite()) {
      throw new FileNotFoundException(
        s"Lineage directory $dir doesn't exist or is not writable.")
    }
  }

  // Visible for testing.
  def createMapper(): ObjectMapper = {
    new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .setSerializationInclusion(Include.NON_NULL)
      .setSerializationInclusion(Include.NON_EMPTY)
      .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
  }

}
