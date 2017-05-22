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

import java.nio.{ByteOrder, ByteBuffer}
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.scalatest.mock.MockitoSugar
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.verify
import parquet.io.api.{Binary, RecordConsumer}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{TimestampType, StructField, StructType}

class ParquetTimestampSuite extends SparkFunSuite with MockitoSugar with SharedSQLContext {

  test("roundtrip via timezone combos") {
    /*
     *  Worked Example
     *  2015-12-31 23:50:59.123 in floating (aka wall-clock aka timestamp without timezone) time
     *  in a few different time zones
     */
    val MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1)
    val TIME_IN_LA = 1451634659123L
    val TIME_IN_CHICAGO = TIME_IN_LA + (-2 * MILLIS_PER_HOUR)
    val TIME_IN_BERLIN = TIME_IN_LA + (-9 * MILLIS_PER_HOUR)
    val initialTz = TimeZone.getDefault()
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
      val conf = new Configuration()
      val catalystSchema = StructType(Array(StructField("ts", TimestampType, true)))
      CatalystWriteSupport.setSchema(catalystSchema, conf)
      conf.set(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY, "America/Chicago")
      conf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "true")
      conf.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
      conf.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")
      val writeSupport = new CatalystWriteSupport()
      val writer = writeSupport.init(conf)

      val recordConsumer = mock[RecordConsumer]
      writeSupport.prepareForWrite(recordConsumer)
      val rowData = new Array[Any](1)
      val row = new GenericInternalRow(rowData)
      val timestampInLa = new java.sql.Timestamp(TIME_IN_LA)
      rowData(0) = CatalystTypeConverters.convertToCatalyst(timestampInLa)
      writeSupport.write(row)
      // Since the test is run in LA tz, we should should convert to the equivalent time in
      // chicago timezone.
      val binaryCaptor = ArgumentCaptor.forClass(classOf[Binary])
      verify(recordConsumer).addBinary(binaryCaptor.capture())
      assert(binaryCaptor.getValue.compareTo(millisToBinary(TIME_IN_CHICAGO)) === 0)

      // If we read that time out again in Berlin timezone, we should convert it to the
      // equivalent time in Berlin.
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"))
      val parquetSchema = new CatalystSchemaConverter(conf).convert(catalystSchema)
      val updater = mock[ParentContainerUpdater]
      val reader = new CatalystRowConverter(parquetSchema, catalystSchema, updater, conf)
      reader.getConverter(0).asPrimitiveConverter().addBinary(millisToBinary(TIME_IN_CHICAGO))
      // We read back time in microseconds, so we need to take millis * 1000
      assert(reader.currentRecord.getLong(0) === (TIME_IN_BERLIN * 1000))
    } finally {
      TimeZone.setDefault(initialTz)
    }
  }

  private def millisToBinary(millis: Long): Binary = {
    val micros = millis * 1000
    val timestampBuffer = new Array[Byte](12)
    val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(micros)
    val buf = ByteBuffer.wrap(timestampBuffer)
    buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)
    Binary.fromByteArray(timestampBuffer)
  }
}
