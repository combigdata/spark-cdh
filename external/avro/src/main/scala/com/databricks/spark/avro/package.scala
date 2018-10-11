// scalastyle:off header
/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on header
package com.databricks.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.avro.IncompatibleSchemaException

package object avro {
  /**
   * Adds a method, `avro`, to DataFrameWriter that allows you to write avro files using
   * the DataFileWriter
   */
  @deprecated("""Use .format("avro") instead.""", "cdh6.1.0")
  implicit class AvroDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def avro: String => Unit = path => {
      try {
        writer.format("avro").save(path)
      } catch {
        case e: IncompatibleSchemaException =>
          throw new SchemaConverters.IncompatibleSchemaException(e.getMessage(), e)
      }
    }
  }

  /**
   * Adds a method, `avro`, to DataFrameReader that allows you to read avro files using
   * the DataFileReader
   */
  @deprecated("""Use .format("avro") instead.""", "cdh6.1.0")
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def avro: String => DataFrame = path => {
      try {
        reader.format("avro").load(path)
      } catch {
        case e: IncompatibleSchemaException =>
          throw new SchemaConverters.IncompatibleSchemaException(e.getMessage(), e)
      }
    }
  }

}
