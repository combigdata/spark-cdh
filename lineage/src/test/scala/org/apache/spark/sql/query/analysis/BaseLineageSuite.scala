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
import java.net.URI
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Try

import com.cloudera.spark.lineage._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

/**
 * Base class for Query analysis testing
 */
abstract class BaseLineageSuite extends ClouderaFunSuite with BeforeAndAfterAll with Logging {

  private var localDir: File = _
  private var hdfs: MiniDFSCluster = _
  private var _spark: SparkSession = _
  private val listener = new TestQeListener()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val baseDir = Utils.createTempDir()
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    this.hdfs = new MiniDFSCluster.Builder(conf).build()

    val builder = SparkSession.builder()
      .config(SparkLauncher.SPARK_MASTER, "local")
      .config(StaticSQLConf.WAREHOUSE_PATH.key, newTempPath(DataSourceType.LOCAL).toString())
    extraSparkConf.foreach { case (k, v) => builder.config(k, v) }
    hdfs.getConfiguration(0).asScala.foreach { e =>
      builder.config(s"spark.hadoop.${e.getKey()}", e.getValue())
    }
    this._spark = builder.getOrCreate()
    _spark.listenerManager.register(listener)

    this.localDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    Try(hdfs.shutdown())
    Try(_spark.stop())
    super.afterAll()
  }

  /** Extra configuration for creating the Spark session. */
  protected def extraSparkConf: Map[String, String] = Map()

  /** The shared Spark session (same session for all tests in the suite). */
  protected final def spark: SparkSession = _spark

  /**
   * Create some temporary files with parquet data and run a function with the file paths as
   * input.
   */
  protected def withInputFiles(
      dsType: DataSourceType,
      count: Int)
      (fn: Seq[String] => Unit): Unit = {
    val inputs = (1 to count).map { i =>
      val path = newTempPath(dsType)

      spark.createDataFrame((1 to 4).map { i => Customer(i, i.toString) })
        .write
        .parquet(path.toString())

      path.toString()
    }

    try {
      fn(inputs)
    } finally {
      val conf = hdfs.getConfiguration(0)
      inputs.foreach { path =>
        FileSystem.get(new URI(path), conf).delete(new Path(path), true)
      }
    }
  }

  /** Creates a temporary file *path* (the underlying file will not exist). */
  protected def newTempPath(dsType: DataSourceType): Path = dsType match {
    case DataSourceType.LOCAL =>
      val file = File.createTempFile("local_", ".parquet", localDir)
      file.delete()
      new Path(file.toURI())

    case DataSourceType.HDFS =>
      hdfs.getFileSystem().makeQualified(new Path("hdfs_" + UUID.randomUUID()))

    case _ =>
      throw new UnsupportedOperationException("NotImplemented")
  }

  protected def createTable(table: String, cols: Map[String, String]): Unit = {
    val colString = cols.map(kv => s"${kv._1} ${kv._2}").mkString(",")
    spark.sql(s"""create table $table ($colString) stored as parquet""")
  }

  /** Creates a hive relation instance. The default db name is prepended if a db is not detected. */
  protected def hiveRelation(
      name: String,
      cols: Seq[String],
      format: DataSourceFormat = DataSourceFormat.PARQUET): RelationInfo = {
    val qualifiedName = if (name.indexOf(".") < 0) s"default.$name" else name
    relation(qualifiedName, cols, DataSourceType.HIVE, format = format)
  }

  protected def hdfsRelation(
      name: String,
      cols: Seq[String],
      format: DataSourceFormat = DataSourceFormat.UNKNOWN): RelationInfo = {
    relation(name, cols, DataSourceType.HDFS, format = format)
  }

  protected def localRelation(
      name: String,
      cols: Seq[String],
      format: DataSourceFormat = DataSourceFormat.UNKNOWN): RelationInfo = {
    relation(name, cols, DataSourceType.LOCAL, format = format)
  }

  protected def relation(
      source: String,
      cols: Seq[String],
      rtype: DataSourceType,
      format: DataSourceFormat = DataSourceFormat.UNKNOWN): RelationInfo = {
    RelationInfo(source, cols, rtype, format, None)
  }

  /** Shortcut for assertLineage(Seq, Seq) for queries with a single output and 0 or more inputs. */
  protected def assertLineage(output: RelationInfo, inputs: RelationInfo*): Unit = {
    assertLineage(Seq(output), inputs)
  }

  /** Shortcut for queries that don't have outputs. */
  protected def assertInputOnly(inputs: RelationInfo*): Unit = {
    assertLineage(Nil, inputs)
  }

  protected def assertLineage(outputs: Seq[RelationInfo], inputs: Seq[RelationInfo]): Unit = {
    val info = listener.getAndClear()

    def toMap(rels: Seq[RelationInfo]): Map[String, RelationInfo] = {
      rels.map { r => (r.source, r) }.toMap
    }

    // Instead of comparing case classes directly, compare fields individually so that:
    // - error messages are easier to parse
    // - lists can be compared in a way in which order is not an issue.
    def compare(rels: Seq[RelationInfo], expRels: Seq[RelationInfo]): Unit = {
      logDebug(s"""|
        |Comparing lineage info:
        |  found: $rels
        |  expected: $expRels
        |""".stripMargin)

      val expected = toMap(expRels)
      val collected = toMap(rels)

      expected.foreach { case (name, r) =>
        collected.get(name) match {
          case Some(fromLineage) =>
            assert(fromLineage.source === r.source)
            assert(fromLineage.fields.toSet === r.fields.toSet)
            assert(fromLineage.dataSourceType === r.dataSourceType)
            assert(fromLineage.dataSourceFormat === r.dataSourceFormat)
            assert(fromLineage.hiveMetastoreLocation === r.hiveMetastoreLocation)

          case None =>
            fail(s"Expected relation $name not found in collected lineage.")
        }
      }

      val unexpected = collected.keys.toSet -- expected.keys
      assert(unexpected.isEmpty, s"Found unexpected relations $unexpected in lineage.")
    }

    compare(info.outputs, outputs)
    compare(info.inputs, inputs)
  }

  private class TestQeListener extends QueryExecutionListener {
    private var qe: QueryExecution = _

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      this.qe = qe
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

    def getAndClear(): LineageInfo = {
      val rQe = qe
      qe = null
      assert(rQe != null)
      QueryAnalysis.getLineageInfo(rQe)
    }
  }

}

case class Customer(id: Int, name: String) extends Product
