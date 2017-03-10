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

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.query.analysis.DataSourceType.DataSourceType
import org.apache.spark.sql.util.QueryExecutionListener

object TestQeListener extends QueryExecutionListener {
  private var qe: QueryExecution = _

  override def onSuccess( funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    this.qe = qe
  }

  override def onFailure( funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  def getAndClear(): QueryExecution = {
    val rQe = qe
    qe = null
    rQe
  }
}

object TestUtils {
  def assertHiveFieldExists(
      inputMetadata: List[FieldDetails],
      table: String,
      column: String,
      db: String = "default"): Unit = {
    val fd = FieldDetails(Array(db + "." + table), column,
      DataSourceType.HIVE, DataSourceFormat.UNKNOWN)
    assert(inputMetadata.contains(fd), s"$inputMetadata doesn't contain: $fd")
  }

  def assertHDFSFieldExists(
      inputMetadata: List[FieldDetails],
      parquetFiles: Array[String],
      column: String,
      dataSourceType: DataSourceType): Unit = {
    val fd = FieldDetails(parquetFiles.map(getScheme(dataSourceType) + _), column,
      dataSourceType, DataSourceFormat.UNKNOWN)
    assert(inputMetadata.contains(fd), s"$inputMetadata doesn't contain: $fd")
  }

  def getScheme(dataSourceType: DataSourceType): String = {
    if (dataSourceType == DataSourceType.LOCAL) "file:" else ""
  }
}
