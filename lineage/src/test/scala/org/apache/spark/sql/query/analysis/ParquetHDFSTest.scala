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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * Base class for Query analysis testing
 */
trait ParquetHDFSTest {

  case class Customer(id: Int, name: String) extends Product

  private def createHDFSCluster: MiniDFSCluster = {
    val baseDir = Utils.createTempDir().getCanonicalFile
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    new MiniDFSCluster.Builder(conf).build()
  }

  protected def withHDFSFile(f: String => Unit): Unit = {
    val hdfs = createHDFSCluster.getFileSystem
    val path = hdfs.makeQualified(new Path("hdfs_" + UUID.randomUUID()))
    try {
      f(path.toString)
    } finally {
      hdfs.delete(path, true)
    }
  }

  protected def withParquetHDFSFile[T <: Product: ClassTag: TypeTag](data: Seq[T])(
      f: String => Unit): Unit = {
    withHDFSFile { hdfsFile =>
      sqlContext.createDataFrame(data).write.parquet(hdfsFile)
      f(hdfsFile)
    }
  }

  protected def sqlContext: SQLContext
}
