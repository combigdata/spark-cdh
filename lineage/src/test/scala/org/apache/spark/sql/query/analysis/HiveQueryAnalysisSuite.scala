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
import java.util.concurrent.atomic.AtomicLong

import com.cloudera.spark.lineage._
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Tests that check that reading and writing to Hive tables produce the desired lineage data
 */
class HiveQueryAnalysisSuite extends BaseLineageSuite {

  private val TABLE_ID = new AtomicLong()

  private val testTableCols = Seq("code", "description", "total_emp", "salary")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val testTableSchema = testTableCols.zip(Seq("string", "string", "int", "int")).toMap
    createTable("test_table_1", testTableSchema)
    createTable("test_table_2", testTableSchema)

    createTable("employee",
      Map(
        "id" -> "int",
        "first_name" -> "varchar(64)",
        "last_name" -> "varchar(64)",
        "salary" -> "int",
        "address" -> "string",
        "city" -> "varchar(64)"
      )
    )

    createTable("department",
      Map(
        "dept_id" -> "int",
        "name" -> "varchar(64)",
        "location" -> "varchar(64)",
        "budget" -> "int"
      )
    )

    createTable("array_table",
      Map(
        "array_col" -> "array<string>"
      )
    )

    createTable("emp_insert",
      Map(
        "first_name" -> "String",
        "last_name" -> "String",
        "agg_clause" -> "Float",
        "location" -> "String"
      )
    )
  }

  override protected def extraSparkConf: Map[String, String] = {
    Map(
      StaticSQLConf.CATALOG_IMPLEMENTATION.key -> "hive",
      "spark.hadoop." + HiveConf.ConfVars.METASTORECONNECTURLKEY.varname ->
        s"jdbc:derby:memory:;create=true")
  }

  test("simple queries") {
    val df = spark.sql("select code, description, salary from test_table_1")
    val input = hiveRelation("default.test_table_1", Seq("code", "description", "salary"))

    df.collect()
    assertInputOnly(input)

    withOutputName { name =>
      df.write.saveAsTable(name)
      assertLineage(hiveRelation(name, Seq("code", "description", "salary")), input)
    }
  }

  test("complex joins") {
    val df1 = spark.sql("""
      select
        code,
        sal
      from (
        select
          o.code as code,
          c.description as desc,
          c.salary as sal
        from
          test_table_1 c
        join
          test_table_2 o
        on
          (c.code = o.code)
        where
          c.salary > 170000
        sort by sal
      ) t1
      limit 3
      """
    )
    val inputs = Seq(
      hiveRelation("default.test_table_1", Seq("salary")),
      hiveRelation("default.test_table_2", Seq("code"))
    )

    val df2 = df1.filter(df1("sal") > 100000)

    df2.collect()
    assertInputOnly(inputs: _*)

    withOutputName { name =>
      df2.write.saveAsTable(name)
      assertLineage(hiveRelation(name, Seq("code", "sal")), inputs: _*)
    }
  }

  test("* queries") {
    withOutputName { name =>
      spark.sql("select * from test_table_1")
        .write
        .saveAsTable(name)

      assertLineage(
        hiveRelation(name, testTableCols),
        hiveRelation("default.test_table_1", testTableCols)
      )
    }
  }

  test("output metadata has fully qualified table name") {
    val db = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    withOutputName { name =>
      try {
        spark.sql(s"create database $db")
        spark.sql(s"use $db")

        spark.sql("select * from default.test_table_1")
          .write
          .saveAsTable(name)
        assertLineage(
          hiveRelation(s"$db.$name", testTableCols),
          hiveRelation("test_table_1", testTableCols)
        )
      } finally {
        spark.sql(s"use default")
      }
    }
  }

  test("CDH-50079: a hive table joined with a parquet temp table is listed correctly") {
    withInputFiles(DataSourceType.HDFS, 1) { case Seq(prq) =>
      spark.read.parquet(prq).createOrReplaceTempView("customers")
      withOutputName { name =>
        spark.sql("""
          select
            test_table_1.code,
            customers.name
          from
            test_table_1
          join
            customers
          where
            test_table_1.code = customers.id
            and test_table_1.description = 'Tom Cruise'
          """)
          .write.saveAsTable(name)
        assertLineage(
          hiveRelation(name, Seq("code", "name")),
          hiveRelation("test_table_1", Seq("code")),
          hdfsRelation(prq, Seq("name"), format = DataSourceFormat.PARQUET)
        )
      }
    }
  }

  test("CDH-50366: output from in-memory data frame") {
    val input = inMemoryInput()
    val inputCols = input.schema.fields.map(_.name)

    withOutputName { name =>
      input.write.saveAsTable(name)
      assertLineage(hiveRelation(name, inputCols))
    }

    val outputPath = newTempPath(DataSourceType.LOCAL).toString()
    inMemoryInput().write.json(outputPath)
    assertLineage(localRelation(outputPath, inputCols, format = DataSourceFormat.JSON))
  }

  test("CDH-50366: join with in-memory data frame") {
    val inMemory = inMemoryInput()
    val inputCols = inMemory.schema.fields.map(_.name)

    val hiveView = spark.sql("select code, description, salary from test_table_1")
    val joined = hiveView.join(inMemory, inMemory.col("_3") === hiveView.col("salary"))

    withOutputName { name =>
      joined.write.saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("code", "description", "salary") ++ inputCols),
        hiveRelation("test_table_1", Seq("salary", "code", "description"))
      )
    }

    withOutputName { name =>
      val projection = inputCols.take(2) ++ Seq("salary", "code")
      joined.select(projection(0), projection.drop(1): _*).write.saveAsTable(name)
      assertLineage(
        hiveRelation(name, projection),
        hiveRelation("test_table_1", Seq("salary", "code"))
      )
    }

    val persons = spark.createDataFrame((1 to 4).map { i => Person(i.toString, i) })

    withOutputName { name =>
      persons.write.saveAsTable(name)
      assertLineage(hiveRelation(name, Seq("name", "age")))
    }

    withOutputName { name =>
      persons.join(inMemory, inMemory.col("_3") === persons.col("age"))
        .write
        .saveAsTable(name)
      assertLineage(hiveRelation(name, Seq("name", "age") ++ inputCols))
    }

    withOutputName { name =>
      persons.join(hiveView, hiveView.col("salary") === persons.col("age"))
        .select("code", "description", "name", "age")
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("code", "description", "name", "age")),
        hiveRelation("test_table_1", Seq("code", "description"))
      )
    }
  }

  test("CDH-51351: CTAS queries should report lineage") {
    withOutputName { name =>
      spark.sql("select * from test_table_1")
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, testTableCols),
        hiveRelation("test_table_1", testTableCols)
      )
    }

    withOutputName { name =>
      spark.sql(s"create table $name as select * from test_table_1")
      assertLineage(
        hiveRelation(name, testTableCols, format = DataSourceFormat.UNKNOWN),
        hiveRelation("test_table_1", testTableCols)
      )
    }

    withOutputName { name =>
      spark.sql(s"create table $name as select code, salary from test_table_1")
      assertLineage(
        hiveRelation(name, Seq("code", "salary"), format = DataSourceFormat.UNKNOWN),
        hiveRelation("test_table_1", Seq("code", "salary"))
      )
    }

    withOutputName { name =>
      spark.sql(s"""
        create table $name as
          select
            code, sal
          from (
            select
              t2.code as code,
              t1.description as desc,
              t1.salary as sal
            from
              test_table_1 t1
            join
              test_table_2 t2
            on
              t1.code = t2.code
            where
              t1.salary > 170000
            sort by sal
          ) t
          limit 3
        """)
      assertLineage(
        hiveRelation(name, Seq("code", "sal"), format = DataSourceFormat.UNKNOWN),
        hiveRelation("test_table_1", Seq("salary")),
        hiveRelation("test_table_2", Seq("code"))
      )
    }
  }

  test("CDH-51296: insert queries should report lineage") {
    withOutputName { name =>
      // Create test table of different column names
      createTable(name, Map("code_new" -> "STRING", "salary_new" -> "INT"))

      // Assert select * of tables of similar schema works for DataFrameWriter.insert method
      spark.sql("select * from test_table_1").write.insertInto("test_table_2")
      assertLineage(
        hiveRelation("test_table_2", testTableCols),
        hiveRelation("test_table_1", testTableCols)
      )

      // Assert select where column name of input table is different from column names of output
      // table for DataFrameWriter.insert method
      spark.sql("select code, salary from test_table_1").write.insertInto(name)
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("code", "salary"))
      )

      // Assert select * works where output table column names vary from input table column names
      spark.sql("select * from (select code, salary from test_table_1)")
        .write
        .insertInto(name)
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("code", "salary"))
      )

      // Assert insert with complex join query
      spark.sql("""
        select
          code, sal
        from (
          select
            tt_2.code as code,
            tt_1.description as desc,
            tt_1.salary as sal
          from
            test_table_1 tt_1
          join
            test_table_2 tt_2
          on
            tt_1.code = tt_2.code
          where
            tt_1.salary > 170000
          sort by sal
        ) t1
        limit 3
        """).write.insertInto(name)
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("salary")),
        hiveRelation("test_table_2", Seq("code"))
      )

      // Repeat the above same tests for insert into query
      // Assert select * of tables of similar schema works insert into query
      spark.sql("insert into test_table_2 select * from test_table_1")
      assertLineage(
        hiveRelation("test_table_2", testTableCols),
        hiveRelation("test_table_1", testTableCols)
      )

      // Assert select query where column name of input table is different from column names of
      // output table works for insert into query
      spark.sql(s"insert into $name select code, salary from test_table_1")
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("code", "salary"))
      )

      // Assert select * works where output table column names vary from input table column names
      spark.sql(s"insert into $name select * from (select code, salary from test_table_1)")
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("code", "salary"))
      )

      // Assert select query with complex join works for insert into query
      spark.sql(s"""
        insert into
          $name
        select
          code, sal
        from (
          select
            tt_2.code as code,
            tt_1.description as desc,
            tt_1 .salary as sal
          from
            test_table_1 tt_1
          join
            test_table_2 tt_2
          on
            tt_1.code = tt_2.code
          where
            tt_1.salary > 170000
          sort by sal
        ) t1
        limit 3
        """)
      assertLineage(
        hiveRelation(name, Seq("code_new", "salary_new")),
        hiveRelation("test_table_1", Seq("salary")),
        hiveRelation("test_table_2", Seq("code"))
      )
    }
  }

  test("CDH-56549: lineage when using functions") {
    withOutputName { name =>
      spark.sql("select lower(city) as lcity from employee").write.saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("lcity")),
        hiveRelation("employee", Seq("city"))
      )
    }

    withOutputName { name =>
      spark.sql("select concat(city, address) as result from employee").write.saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("result")),
        hiveRelation("employee", Seq("city", "address"))
      )
    }
  }

  test("CDH-51486: simple aggregation") {
    withOutputName { name =>
      spark
        .sql("select city, address, avg(salary) as avgsal from employee group by city, address")
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("city", "address", "avgsal")),
        hiveRelation("employee", Seq("salary", "city", "address"))
      )
    }
  }

  test("CDH-51486: aggregation using DF API") {
    withOutputName { name =>
      val df1 = spark
        .sql("select city, address, salary from employee")
        .groupBy("city", "address")
        .avg("salary")
      df1.withColumnRenamed(df1.columns(2), "avgsal")
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("city", "address", "avgsal")),
        hiveRelation("employee", Seq("city", "address", "salary"))
      )
    }
  }

  test("CDH-51486: multiple aggregations using DF API") {
    withOutputName { name =>
      val df1 = spark
        .sql("select city, salary, id from employee")
        .groupBy("city")
        .agg("salary" -> "avg", "id" -> "sum")
      df1
        .withColumnRenamed(df1.columns(1), "avgsal")
        .withColumnRenamed(df1.columns(2), "sumid")
        .write
        .saveAsTable(name)

      assertLineage(
        hiveRelation(name, Seq("city", "avgsal", "sumid")),
        hiveRelation("employee", Seq("city", "salary", "id"))
      )
    }
  }

  test("CDH-51486: aggregation of UDF result") {
    withOutputName { name =>
      spark
        .sql("""
          select
            city,
            address,
            sum(ceil(salary)) as thesum
          from
            employee
          group by city, address
          """)
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("city", "address", "thesum")),
        hiveRelation("employee", Seq("salary", "city", "address"))
      )
    }
  }

  test("CDH-51486: aggregation accepting multiple columns as input") {
    withOutputName { name =>
      spark
        .sql("""
          select
            covar_pop(budget, dept_id) as aggClause,
            location,
            name
          from
            department
          group by location, name
          limit 15""")
        .write
        .saveAsTable(name)

      assertLineage(
        hiveRelation(name, Seq("aggClause", "location", "name")),
        hiveRelation("department", Seq("location", "name", "budget", "dept_id"))
      )
    }
  }

  test("CDH-51486: aggregation with multiple UDAFs in the same query") {
    withOutputName { name =>
      spark
        .sql("""
          select
            sum(dept_id) as meaningless_sum,
            avg(budget) as avgbudget,
            location,
            name
          from
            department
          group by location, name
          limit 15
          """)
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("meaningless_sum", "avgbudget", "location", "name")),
        hiveRelation("department", Seq("location", "name", "budget", "dept_id"))
      )
    }
  }

  test("subquery") {
    withOutputName { name =>
      spark
        .sql("""
          select
            fn
          from (
            select
              upper(first_name) as fn
            from
              employee
          ) t
          """)
        .write
        .saveAsTable(name)

      assertLineage(
        hiveRelation(name, Seq("fn")),
        hiveRelation("employee", Seq("first_name"))
      )
    }
  }

  test("join with nested subquery") {
    withOutputName { name =>
      spark
        .sql("""
          select
            emp.first_name,
            emp.last_name,
            emp.city,
            dept.location
          from
            employee emp
          join
            (
              select
                *
              from
                department
              limit 15
            ) dept
          on
            emp.city = dept.location
          """)
        .write
        .saveAsTable(name)

      assertLineage(
        hiveRelation(name, Seq("first_name", "last_name", "city", "location")),
        hiveRelation("employee", Seq("first_name", "last_name", "city")),
        hiveRelation("department", Seq("location"))
      )
    }
  }

  test("UDTF") {
    withOutputName { name =>
      spark.sql("select explode(array_col) as exploded from array_table")
        .write
        .saveAsTable(name)
      assertLineage(
        hiveRelation(name, Seq("exploded")),
        hiveRelation("array_table", Seq("array_col"))
      )
    }
  }

  test("non-Hive compatible output") {
    withOutputName { name =>
      val path = newTempPath(DataSourceType.LOCAL).toString()
      spark.sql("""
        select
          emp.first_name,
          emp.last_name,
          emp.city,
          dept.location
        from
          employee emp
        join
          (
            select
              *
            from
              department
            limit 15
          ) dept
        on
          emp.city = dept.location
         """)
         .write
         .format("json")
         .option("path", path)
         .saveAsTable(name)
      assertLineage(
        localRelation(path, Seq("first_name", "last_name", "city", "location"),
          format = DataSourceFormat.JSON),
        hiveRelation("employee", Seq("first_name", "last_name", "city")),
        hiveRelation("department", Seq("location"))
      )
    }
  }

  private def testAggregate(inputs: Seq[String], clause: String): Unit = {
    test(s"CDH-57411: complex joins with aggregate ($clause)") {
      withOutputName { name =>
        spark.sql(s"""
          select
            emp.first_name,
            emp.last_name,
            aggClause,
            location
          from
            employee emp
          join
            (
              select
                $clause as aggClause,
                location
              from
                department
              group by location, dept_id
              sort by location
              limit 15
            ) dept
          on
            emp.city = dept.location
          """)
          .write
          .saveAsTable(name)

        assertLineage(
          hiveRelation(name, Seq("first_name", "last_name", "aggClause", "location")),
          hiveRelation("department", inputs ++ Seq("location")),
          hiveRelation("employee", Seq("first_name", "last_name"))
        )
      }
    }

    test(s"CDH-57411: ctas with aggregate ($clause)") {
      withOutputName { name =>
        spark.sql(s"""
          create table $name as select
            emp.first_name,
            emp.last_name,
            aggClause,
            location
          from
            employee emp
          join
            (
              select
                $clause as aggClause,
                location
              from
                department
              group by location, dept_id
              sort by location
              limit 15
            ) dept
          on
            emp.city = dept.location
          """)

        assertLineage(
          hiveRelation(name, Seq("first_name", "last_name", "aggClause", "location"),
            format = DataSourceFormat.UNKNOWN),
          hiveRelation("department", inputs ++ Seq("location")),
          hiveRelation("employee", Seq("first_name", "last_name"))
        )
      }
    }

    test(s"CDH-57411: insert with aggregate ($clause)") {
      spark.sql(s"""
        insert into
          emp_insert
        select
          emp.first_name as fname,
          emp.last_name as lname,
          aggClauseInsert,
          location
        from
          employee emp
        join
          (
            select
              $clause as aggClauseInsert,
              location
            from
              department
            group by location, dept_id
            sort by location
            limit 15
          ) dept
        on
        emp.city = dept.location
        """)

      assertLineage(
        hiveRelation("emp_insert", Seq("first_name", "last_name", "agg_clause", "location")),
        hiveRelation("department", inputs ++ Seq("location")),
        hiveRelation("employee", Seq("first_name", "last_name"))
      )
    }
  }

  Seq(
    Seq("budget") -> unaryAggregates("budget"),
    Seq("budget", "dept_id") -> binaryAggregates("budget", "dept_id"),
    Nil -> Seq("count(*)")
  ).foreach { case (cols, aggs) =>
    aggs.foreach { agg =>
      testAggregate(cols, agg)
    }
  }

  test("NAV-5318: projection containing join key") {
    val asql = s"""SELECT A.city as city, A.last_name FROM employee A"""
    val bsql = s"""SELECT B.location as city, B.name FROM department B"""
    val left = spark.sql(asql)
    val right = spark.sql(bsql)
    Seq(false, true).foreach { bcast =>
      left.join(if (bcast) { broadcast(right) } else { right }, Seq("city"), "left_outer")
        .collect()

      assertInputOnly(
        hiveRelation("employee", Seq("last_name", "city")),
        hiveRelation("department", Seq("name"))
      )
    }
  }

  Map(
    "parquet" -> DataSourceFormat.PARQUET,
    "avro" -> DataSourceFormat.AVRO,
    "textfile" -> DataSourceFormat.UNKNOWN)
  .foreach { case (storage, format) =>
    test(s"Hive table format detection: $storage") {
      withOutputName { name =>
        spark.sql(s"""CREATE TABLE $name STORED AS $storage AS SELECT * FROM test_table_1""")
        assertLineage(
          hiveRelation(name, testTableCols, format = format),
          hiveRelation("test_table_1", testTableCols)
        )
      }
    }
  }

  test("CDH-50106: aliases and casts using df api") {
    val _spark = spark
    import _spark.implicits._

    withInputFiles(DataSourceType.HDFS, 1) { case Seq(prq) =>
      withOutputName { name =>
        spark.read.parquet(prq)
          .withColumn("aliasedId", 'id.cast(IntegerType))
          .select('aliasedId.alias("anotherAlias"), 'name)
          .write
          .saveAsTable(name)

        assertLineage(
          hiveRelation(name, Seq("anotherAlias", "name")),
          hdfsRelation(prq, Seq("id", "name"), format = DataSourceFormat.PARQUET)
        )
      }
    }
  }

  test("dataframe caching") {
    val df = spark.sql("select * from test_table_1").cache()
    df.count()

    Seq(1, 2).foreach { idx =>
      withOutputName { name =>
        df.select(testTableCols(idx)).write.saveAsTable(name)
        assertLineage(
          hiveRelation(name, Seq(testTableCols(idx))),
          hiveRelation("test_table_1", Seq(testTableCols(idx)))
        )
      }
    }
  }

  test("CDH-60505: joining datasets with clashing column names") {
    withOutputName { name =>
      val left = spark.sql("SELECT a.code as code, a.description FROM test_table_1 a")
      val right = spark.sql("SELECT b.total_emp as total_emp, b.salary, b.code FROM test_table_2 b")
      left.join(right, Seq("code"), "left_outer").write.saveAsTable(name)

      assertLineage(
        hiveRelation(name, testTableCols),
        hiveRelation("test_table_1", Seq("code", "description")),
        hiveRelation("test_table_2", Seq("total_emp", "salary"))
      )
    }
  }

  private def unaryAggregates(col: String): Seq[String] = {
    Seq(
      s"sum($col)",
      s"avg($col)",
      s"stddev_pop($col)",
      s"percentile($col, 0)"
    )
  }

  private def binaryAggregates(col1: String, col2: String): Seq[String] = {
    Seq(
      s"covar_pop($col1, $col2)",
      s"corr($col1, $col2)"
    )
  }

  private def inMemoryInput(): DataFrame = {
    spark.createDataFrame((1 to 4).map { i =>
      (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
    })
  }

  /**
   * Run a block of code with a unique table name as input.
   */
  private def withOutputName(fn: String => Unit) = {
    val id = TABLE_ID.incrementAndGet()
    fn(s"test_output_${id}")
  }

}

case class Person(name: String, age: Long)
