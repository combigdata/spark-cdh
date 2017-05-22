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

package org.apache.spark.sql.hive

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompatibilityTest, ParquetFileFormat}
import org.apache.spark.sql.hive.client.HiveTable
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

class ParquetHiveCompatibilitySuite extends ParquetCompatibilityTest with TestHiveSingleton {
  /**
   * Set the staging directory (and hence path to ignore Parquet files under)
   * to that set by [[HiveConf.ConfVars.STAGINGDIR]].
   */
  private val stagingDir = new HiveConf().getVar(HiveConf.ConfVars.STAGINGDIR)

  // This is needed just for backporting to the spark 1.6 line, to access the table properties.
  // spark 2.x exposes the table metadata directly via catalog.
  private lazy val hiveCatalog = sqlContext.catalog.asInstanceOf[HiveMetastoreCatalog]
  private def hiveTable(tableId: TableIdentifier): HiveTable = {
    val database = tableId.database.getOrElse(hiveCatalog.client.currentDatabase).toLowerCase
    hiveCatalog.client.getTable(database, tableId.table)
  }

  override protected def logParquetSchema(path: String): Unit = {
    val schema = readParquetSchema(path, { path =>
      !path.getName.startsWith("_") && !path.getName.startsWith(stagingDir)
    })

    logInfo(
      s"""Schema of the Parquet file written by parquet-avro:
         |$schema
       """.stripMargin)
  }

  private def testParquetHiveCompatibility(row: Row, hiveTypes: String*): Unit = {
    withTable("parquet_compat") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        // Hive columns are always nullable, so here we append a all-null row.
        val rows = row :: Row(Seq.fill(row.length)(null): _*) :: Nil

        // Don't convert Hive metastore Parquet tables to let Hive write those Parquet files.
        withSQLConf(HiveContext.CONVERT_METASTORE_PARQUET.key -> "false") {
          withTempTable("data") {
            val fields = hiveTypes.zipWithIndex.map { case (typ, index) => s"  col_$index $typ" }

            val ddl =
              s"""CREATE TABLE parquet_compat(
                 |${fields.mkString(",\n")}
                 |)
                 |STORED AS PARQUET
                 |LOCATION '$path'
               """.stripMargin

            logInfo(
              s"""Creating testing Parquet table with the following DDL:
                 |$ddl
               """.stripMargin)

            sqlContext.sql(ddl)

            val schema = sqlContext.table("parquet_compat").schema
            val rowRDD = sqlContext.sparkContext.parallelize(rows).coalesce(1)
            sqlContext.createDataFrame(rowRDD, schema).registerTempTable("data")
            sqlContext.sql("INSERT INTO TABLE parquet_compat SELECT * FROM data")
          }
        }

        logParquetSchema(path)

        // Unfortunately parquet-hive doesn't add `UTF8` annotation to BINARY when writing strings.
        // Have to assume all BINARY values are strings here.
        withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
          checkAnswer(sqlContext.read.parquet(path), rows)
        }
      }
    }
  }

  test("simple primitives") {
    testParquetHiveCompatibility(
      Row(true, 1.toByte, 2.toShort, 3, 4.toLong, 5.1f, 6.1d, "foo"),
      "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING")
  }

  test("SPARK-10177 timestamp") {
    testParquetHiveCompatibility(Row(Timestamp.valueOf("2015-08-24 00:31:00")), "TIMESTAMP")
  }

  test("array") {
    testParquetHiveCompatibility(
      Row(
        Seq[Integer](1: Integer, null, 2: Integer, null),
        Seq[String]("foo", null, "bar", null),
        Seq[Seq[Integer]](
          Seq[Integer](1: Integer, null),
          Seq[Integer](2: Integer, null))),
      "ARRAY<INT>",
      "ARRAY<STRING>",
      "ARRAY<ARRAY<INT>>")
  }

  test("map") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          (1: Integer) -> "foo",
          (2: Integer) -> null)),
      "MAP<INT, STRING>")
  }

  // HIVE-11625: Parquet map entries with null keys are dropped by Hive
  ignore("map entries with null keys") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          null.asInstanceOf[Integer] -> "bar",
          null.asInstanceOf[Integer] -> null)),
      "MAP<INT, STRING>")
  }

  test("struct") {
    testParquetHiveCompatibility(
      Row(Row(1, Seq("foo", "bar", null))),
      "STRUCT<f0: INT, f1: ARRAY<STRING>>")
  }

  // Check creating parquet tables, writing data into them, and reading it back out under a
  // variety of conditions:
  // * tables with explicit tz and those without
  // * altering table properties directly
  // * variety of timezones, local & non-local
  testCreateWriteRead("no_tz", None)
  val localTz = TimeZone.getDefault.getID()
  testCreateWriteRead("local", Some(localTz))
  // check with a variety of timezones.  The unit tests currently are configured to always use
  // America/Los_Angeles, but even if they didn't, we'd be sure to cover a non-local timezone.
  Seq(
    "UTC" -> "UTC",
    "LA" -> "America/Los_Angeles",
    "Berlin" -> "Europe/Berlin"
  ).foreach { case (tableName, zone) =>
    if (zone != localTz) {
      testCreateWriteRead(tableName, Some(zone))
    }
  }

  private def testCreateWriteRead(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
    testCreateAlterTablesWithTimezone(baseTable, explicitTz)
    testWriteTablesWithTimezone(baseTable, explicitTz)
    testReadTablesWithTimezone(baseTable, explicitTz)
  }

  private def checkHasTz(table: String, tz: Option[String]): Unit = {
    val tableMetadata = hiveTable(TableIdentifier(table))
    val actualTz = tableMetadata.properties.get(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY)
    assert( actualTz === tz, s"table $table timezone was supposed to be $tz, was $actualTz")
  }

  private def testCreateAlterTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
    test(s"SPARK-12297: Create and Alter Parquet tables and timezones; explicitTz = $explicitTz") {
      // we're cheating a bit here, in general SparkConf isn't meant to be set at runtime,
      // but its OK in this case, and lets us run this test, because these tests don't like
      // creating multiple HiveContexts in the same jvm
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
      withTable(baseTable, s"like_$baseTable", s"select_$baseTable") {
        val localTz = TimeZone.getDefault()
        val localTzId = localTz.getID()
        val defaultTz = None
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => raw"""TBLPROPERTIES ("$key"="$tz")"""
        }.getOrElse("")
        sqlContext.sql(
          raw"""CREATE TABLE $baseTable (
                |  x int
                | )
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        val expectedTableTz = explicitTz.orElse(defaultTz)
        checkHasTz(baseTable, expectedTableTz)
        sqlContext.sql(s"CREATE TABLE like_$baseTable LIKE $baseTable")
        checkHasTz(s"like_$baseTable", expectedTableTz)
        sqlContext.sql(
          raw"""CREATE TABLE select_$baseTable
                | STORED AS PARQUET
                | AS
                | SELECT * from $baseTable
            """.stripMargin)
        checkHasTz(s"select_$baseTable", defaultTz)

        // check alter table, setting, unsetting, resetting the property
        sqlContext.sql(
          raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ("$key"="America/Los_Angeles")""")
        checkHasTz(baseTable, Some("America/Los_Angeles"))
        sqlContext.sql( raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ("$key"="UTC")""")
        checkHasTz(baseTable, Some("UTC"))
        sqlContext.sql( raw"""ALTER TABLE $baseTable UNSET TBLPROPERTIES ("$key")""")
        checkHasTz(baseTable, None)
        explicitTz.foreach { tz =>
          sqlContext.sql( raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ("$key"="$tz")""")
          checkHasTz(baseTable, expectedTableTz)
        }
      }
    }
  }

  val desiredTimestampStrings = Seq(
    "2015-12-31 23:50:59.123",
    "2015-12-31 22:49:59.123",
    "2016-01-01 00:39:59.123",
    "2016-01-01 01:29:59.123"
  )
  // We don't want to mess with timezones inside the tests themselves, since we use a shared
  // spark context, and then we might be prone to issues from lazy vals for timezones.  Instead,
  // we manually adjust the timezone just to determine what the desired millis (since epoch, in utc)
  // is for various "wall-clock" times in different timezones, and then we can compare against those
  // in our tests.
  val originalTz = TimeZone.getDefault
  val timestampTimezoneToMillis = try {
    (for {
      timestampString <- desiredTimestampStrings
      timezone <- Seq("America/Los_Angeles", "Europe/Berlin", "UTC").map {
        TimeZone.getTimeZone(_)
      }
    } yield {
      TimeZone.setDefault(timezone)
      val timestamp = Timestamp.valueOf(timestampString)
      (timestampString, timezone.getID()) -> timestamp.getTime()
    }).toMap
  } finally {
    TimeZone.setDefault(originalTz)
  }

  private def createRawData(): DataFrame = {
    val originalTsStrings = Seq(
      "2015-12-31 23:50:59.123",
      "2015-12-31 22:49:59.123",
      "2016-01-01 00:39:59.123",
      "2016-01-01 01:29:59.123"
    )
    val rowRdd = sqlContext.sparkContext.parallelize(originalTsStrings, 1).map { x =>
      Row(x, java.sql.Timestamp.valueOf(x))
    }
    val schema = StructType(Array(
      StructField("display", StringType, true),
      StructField("ts", TimestampType, true)))
    sqlContext.createDataFrame(rowRdd, schema)
  }

  private def testWriteTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Write to Parquet tables with Timestamps; explicitTz = $explicitTz") {
      withTable(s"saveAsTable_$baseTable", s"insert_$baseTable") {
        val localTz = TimeZone.getDefault()
        val localTzId = localTz.getID()
        val defaultTz = None
        val expectedTableTz = explicitTz.orElse(defaultTz)
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => raw"""TBLPROPERTIES ("$key"="$tz")"""
        }.getOrElse("")


        val rawData = createRawData()
        // Check writing data out.
        // We write data into our tables, and then check the raw parquet files to see whether
        // the correct conversion was applied.
        rawData.write.saveAsTable(s"saveAsTable_$baseTable")
        checkHasTz(s"saveAsTable_$baseTable", defaultTz)
        sqlContext.sql(
          raw"""CREATE TABLE insert_$baseTable (
                |  display string,
                |  ts timestamp
                | )
                | STORED AS PARQUET
                | $tblProperties
               """.stripMargin)
        checkHasTz(s"insert_$baseTable", expectedTableTz)
        rawData.write.insertInto(s"insert_$baseTable")
        val readFromTable = sqlContext.table(s"insert_$baseTable").collect()
          .map(_.toString()).sorted
        // no matter what, roundtripping via the table should leave the data unchanged
        assert(readFromTable === rawData.collect().map(_.toString()).sorted)

        // Now we load the raw parquet data on disk, and check if it was adjusted correctly.
        // Note that we only store the timezone in the table property, so when we read the
        // data this way, we're bypassing all of the conversion logic, and reading the raw
        // values in the parquet file.
        val onDiskLocation = hiveTable(TableIdentifier(s"insert_$baseTable")).location.get
        val readFromDisk = sqlContext.read.parquet(onDiskLocation).collect()
        val storageTzId = explicitTz.getOrElse(TimeZone.getDefault().getID())
        readFromDisk.foreach { row =>
          val displayTime = row.getAs[String](0)
          val millis = row.getAs[Timestamp](1).getTime()
          val expectedMillis = timestampTimezoneToMillis((displayTime, storageTzId))
          assert(expectedMillis === millis)
        }
      }
    }
  }

  private def testReadTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Read from Parquet tables with Timestamps; explicitTz = $explicitTz") {
      withTable(s"external_$baseTable") {
        // we intentionally save this data directly, without creating a table, so we can
        // see that the data is read back differently depending on table properties.
        // we'll save with adjusted millis, so that it should be the correct millis after reading
        // back.
        val localTzID = TimeZone.getDefault().getID()
        val rawData = createRawData()
        // to avoid closing over entire class
        val timestampTimezoneToMillis = this.timestampTimezoneToMillis
        val adjustedRawData = explicitTz match {
          case Some(tzId) =>
            sqlContext.createDataFrame(rawData.map { row =>
              val displayTime = row.getAs[String](0)
              val storageMillis = timestampTimezoneToMillis((displayTime, tzId))
              Row(displayTime, new Timestamp(storageMillis))
            }, rawData.schema)
          case _ =>
            rawData
        }
        withTempPath { path =>
          adjustedRawData.write.parquet(path.getCanonicalPath)
          val tblProperties = explicitTz.map {
            tz => raw"""TBLPROPERTIES ("$key"="$tz")"""
          }.getOrElse("")
          sqlContext.sql(
            raw"""CREATE EXTERNAL TABLE external_$baseTable (
                 | display string,
                 | ts timestamp
                 |)
                 |STORED AS PARQUET
                 |LOCATION '${path.getCanonicalPath}'
                 |$tblProperties
               """.stripMargin)
          val collectedFromExternal =
            sqlContext.sql(s"select display, ts from external_$baseTable").collect()
          collectedFromExternal.foreach { row =>
            val displayTime = row.getAs[String](0)
            val millis = row.getAs[Timestamp](1).getTime()
            assert(millis === timestampTimezoneToMillis((displayTime, localTzID)))
          }

          // Make sure functions applied to the timestamp don't use the storage time, but the
          // converted time.  This is particularly important for partitioned tables, which are often
          // created based on the year, month, day.
          val extractedYear = sqlContext.sql(s"select year(ts) from external_$baseTable").collect()
          assert(extractedYear.map{_.getInt(0)}.sorted === Array(2015, 2015, 2016, 2016))

          sqlContext.read.parquet(path.getCanonicalPath).registerTempTable("raw_data")

          // Now test that the behavior is still correct even with a filter which could get
          // pushed down into parquet.  We don't need special handling for pushed down predicates
          // because we ignore predicates on TimestampType in ParquetFilters.   This check makes
          // sure that doesn't change.
          // These queries should return the entire dataset, but if the predicates were
          // applied to the raw values in parquet, they would incorrectly filter data out.
          Seq(
            ">" -> "2015-12-31 22:00:00",
            "<" -> "2016-01-01 02:00:00"
          ).foreach { case (comparison, value) =>
            val query =
              s"select ts from external_$baseTable where ts $comparison cast('$value' as timestamp)"
            val countWithFilter = sqlContext.sql(query).count()
            assert(countWithFilter === 4, query)
          }
        }
      }
    }
  }

  // TODO CDH-52854.  this depends entirely on hive validating the timezone.  When hive implements
  // HIVE-16469, we should be able to turn this test back on (perhaps tweaking the exact check).
  ignore("SPARK-12297: exception on bad timezone") {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    val badTzException = intercept[AnalysisException] {
      sqlContext.sql(
        raw"""CREATE TABLE bad_tz_table (
              |  x int
              | )
              | STORED AS PARQUET
              | TBLPROPERTIES ("$key"="Blart Versenwald III")
            """.stripMargin)
    }
    assert(badTzException.getMessage.contains("Blart Versenwald III"))
    sqlContext.sql(
      raw"""CREATE TABLE flippidee_floop (
            |  x int
            | )
            | STORED AS PARQUET
            """.stripMargin)
    val badTzAlterException = intercept[AnalysisException] {
      sqlContext.sql(
        raw"""ALTER TABLE flippidee_floop SET TBLPROPERTIES ("$key"="Blart Versenwald III")""")
    }
    assert(badTzAlterException.getMessage.contains("Blart Versenwald III"))
  }
}
