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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File
import java.util.TimeZone

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import parquet.hadoop.ParquetFileReader
import parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetInteroperabilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  test("parquet files with different physical schemas but share the same logical schema") {
    import ParquetCompatibilityTest._

    // This test case writes two Parquet files, both representing the following Catalyst schema
    //
    //   StructType(
    //     StructField(
    //       "f",
    //       ArrayType(IntegerType, containsNull = false),
    //       nullable = false))
    //
    // The first Parquet file comes with parquet-avro style 2-level LIST-annotated group, while the
    // other one comes with parquet-protobuf style 1-level unannotated primitive field.
    withTempDir { dir =>
      val avroStylePath = new File(dir, "avro-style").getCanonicalPath
      val protobufStylePath = new File(dir, "protobuf-style").getCanonicalPath

      val avroStyleSchema =
        """message avro_style {
          |  required group f (LIST) {
          |    repeated int32 array;
          |  }
          |}
        """.stripMargin

      writeDirect(avroStylePath, avroStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.group {
              rc.field("array", 0) {
                rc.addInteger(0)
                rc.addInteger(1)
              }
            }
          }
        }
      })

      logParquetSchema(avroStylePath)

      val protobufStyleSchema =
        """message protobuf_style {
          |  repeated int32 f;
          |}
        """.stripMargin

      writeDirect(protobufStylePath, protobufStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.addInteger(2)
            rc.addInteger(3)
          }
        }
      })

      logParquetSchema(protobufStylePath)

      checkAnswer(
        sqlContext.read.parquet(dir.getCanonicalPath),
        Seq(
          Row(Seq(0, 1)),
          Row(Seq(2, 3))))
    }
  }

  test("parquet timestamp conversion") {
    // Make a table with one parquet file written by impala, and one parquet file written by spark.
    // We should only adjust the timestamps in the Impala file, and only if the conf is set
    val impalaFile = "test-data/impala_timestamp.parq"

    // here are the timestamps in the impala file, as they were saved by impala
    val impalaFileData =
      Seq(
        "2001-01-01 01:01:01",
        "2002-02-02 02:02:02",
        "2003-03-03 03:03:03"
      ).map(java.sql.Timestamp.valueOf)
    val impalaPath =
      Thread.currentThread().getContextClassLoader.getResource(impalaFile) .toURI.getPath
    withTempPath { tableDir =>
      val ts = Seq(
        "2004-04-04 04:04:04",
        "2005-05-05 05:05:05",
        "2006-06-06 06:06:06"
      ).map { java.sql.Timestamp.valueOf }
      import testImplicits._
      val df = ts.map{ s => Tuple1(s) }.toDF().repartition(1).withColumnRenamed("_1", "ts")
      df.write.parquet(tableDir.getAbsolutePath)
      FileUtils.copyFile(new File(impalaPath), new File(tableDir, "part-00001.parq"))
      Seq(false, true).foreach { int96TimestampConversion =>
        withSQLConf(
          (SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key, int96TimestampConversion.toString())) {
          val readBack = sqlContext.read.parquet(tableDir.getAbsolutePath).collect()
          assert(readBack.size === 6)
          // if we apply the conversion, we'll get the "right" values, as saved by impala in the
          // original file.  Otherwise, they're off by the local timezone offset.
          val impalaExpectations = if (int96TimestampConversion) {
            impalaFileData
          } else {
            impalaFileData.map { ts =>
              DateTimeUtils.toJavaTimestamp(DateTimeUtils.convertTz(
                DateTimeUtils.fromJavaTimestamp(ts),
                DateTimeUtils.TimeZoneUTC,
                TimeZone.getDefault))
            }
          }
          val fullExpectations = (ts ++ impalaExpectations).map(_.toString).sorted.toArray
          val actual = readBack.map(_.getTimestamp(0).toString).sorted

          withClue(s"int96TimestampConversion = $int96TimestampConversion;") {
            assert(fullExpectations === actual)

            // Now test that the behavior is still correct even with a filter which could get
            // pushed down into parquet.  We don't need extra handling for pushed down
            // predicates because in ParquetFilters, we ignore TimestampType.
            //
            // Upstream we also check that column statistics are ignored, but in CDH5.x
            // column statistics are read for unsigned types, so we omit that check
            // here. Spark does not evaluate predicates for timestamp against column stats
            // in CDH5.x anyhow.
            //
            // These queries should return the entire dataset with the conversion applied,
            // but if the predicates were applied to the raw values in parquet, they would
            // incorrectly filter data out.
            val query = sqlContext.read.parquet(tableDir.getAbsolutePath)
              .where("ts > '2001-01-01 01:00:00'")
            val countWithFilter = query.count()
            val exp = if (int96TimestampConversion) 6 else 5
            assert(countWithFilter === exp, query)
          }
        }
      }
    }
  }

}
