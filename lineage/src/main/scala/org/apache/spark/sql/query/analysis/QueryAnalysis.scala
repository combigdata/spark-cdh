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

import java.net.URI

import scala.collection.mutable

import com.cloudera.spark.lineage._
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.annotation.Private
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.internal.HiveSerDe

/**
 * This class is responsible for analyzing the {@link QueryExecution} and extracting the input
 * and output metadata. It has to live in the Spark SQL namespace since it accesses some internal
 * Spark methods not available outside of those packages.
 */
@Private
object QueryAnalysis extends Logging {

  /** Keep a map of serde-to-format mapping for recognized formats. */
  private val hiveSerdesMapping = Seq(
    HiveSerDe.serdeMap("parquet") -> DataSourceFormat.PARQUET,
    HiveSerDe.serdeMap("avro") -> DataSourceFormat.AVRO)

  /**
   * Computes the lineage information from a query execution.
   *
   * @param qe QueryExecution object.
   * @return Query lineage information if the query is a supported output command.
   */
  def getLineageInfo(qe: QueryExecution): LineageInfo = {
    logDebug(s"computing lineage info for:\n${qe.optimizedPlan}")

    // Stash the location of the Hive metastore so that it can be saved with any Hive relations
    // that are referenced in the query.
    val metastore = qe.sparkSession.sessionState.catalog.externalCatalog match {
      case h: HiveExternalCatalog =>
        val uris = h.client.getConf(HiveConf.ConfVars.METASTOREURIS.varname, null)
        if (uris != null && uris.nonEmpty) {
          Some(uris)
        } else {
          None
        }

      case _ =>
        None
    }

    val inputs = new LineageInputs()
    val outputs = findOutputInfo(qe.optimizedPlan, qe.sparkSession) match {
      case Some((output, children)) =>
        // There is an output command in the plan, so use its children as the initial field
        // list in the input mapping.
        output.fields.foreach { f => inputs.fieldMappings(f.exprId) = Seq(f) }
        children.foreach { c => collectLineageInfo(c, inputs, qe.sparkSession) }

        val lineageOutput = {
          val _metastore = if (output.dsType == DataSourceType.HIVE) metastore else None
          RelationInfo(output.source, output.fields.map(_.name), output.dsType, output.dsFormat,
            _metastore)
        }
        Seq(lineageOutput)

      case None =>
        // Query doesn't have any output - the results are processed by the application itself.
        // Use the top-level plan to define which are the "output" columns.
        qe.optimizedPlan.output.foreach { f =>
          inputs.fieldMappings(f.exprId) = Seq(f)
        }
        qe.optimizedPlan.children.foreach { c => collectLineageInfo(c, inputs, qe.sparkSession) }
        Nil
    }

    val inputRels = inputs.relations.map { case (_, inputRel) =>
      // Collect only the columns of the relation that match the output.
      val inputCols = inputRel.fields.filter(inputs.isSourceField)

      // If it's a Hive relation, set the metastore information.
      val _metastore = if (inputRel.dsType == DataSourceType.HIVE) metastore else None

      RelationInfo(inputRel.source, inputCols.map(_.name), inputRel.dsType, inputRel.dsFormat,
        _metastore)
    }

    LineageInfo(outputs, inputRels.toSeq)
  }

  /**
   * Find the plan's output relation, and its list of children so that its inputs can be found.
   */
  private def findOutputInfo(
      plan: LogicalPlan,
      spark: SparkSession): Option[(SQLRelationInfo, Seq[LogicalPlan])] = plan match {
    case CreateDataSourceTableAsSelectCommand(table, _, query) =>
      getOutputInfo(table, query, spark)

    case InsertIntoHadoopFsRelationCommand(path, _, _, parts, _, fmt, _, query, _, table, _, _) =>
      val rel = table
        .flatMap { t => getRelationInfo(t.identifier, spark, query.output) }
        .getOrElse {
          val dsType = getDataSourceType(path.toUri())
          val dsFormat = table.map(getDataSourceFormat).getOrElse(getDataSourceFormat(fmt))
          SQLRelationInfo(path.toString(), query.output, dsType, dsFormat)
        }
      Some((rel, Seq(query)))

    case CreateHiveTableAsSelectCommand(table, query, _) =>
      getOutputInfo(table, query, spark)

    case InsertIntoHiveTable(table, _, query, _, _, _) =>
      getOutputInfo(table, query, spark)

    case _ => None
  }

  private def getOutputInfo(
      table: CatalogTable,
      query: LogicalPlan,
      spark: SparkSession): Option[(SQLRelationInfo, Seq[LogicalPlan])] = {
    getRelationInfo(table.identifier, spark, query.output).map { rel =>
      (rel, Seq(query))
    }
  }

  /**
   * Collects lineage information from a plan, recursively.
   *
   * The initial partial lineage info should have the output fields populated in the `fieldMappings`
   * map, mapping the fields to itself as the only known alias. The mapping will be updated as the
   * plan is traversed.
   */
  private def collectLineageInfo(
      plan: LogicalPlan,
      inputs: LineageInputs,
      spark: SparkSession): Unit = {
    logDebug(s"collecting lineage input from:\n$plan")

    // Verify whether the plan creates a mapping between input and output names, and update the
    // mappings in the partial info.
    plan match {
      case Generate(gen, _, _, _, output, child) =>
        val sources = flattenReferences(gen)
        output.foreach {
          case e: NamedExpression =>
            inputs.replace(e, sources)

          case _ =>
        }

      case Project(exprs, _) =>
        processAliases(exprs, inputs)

      case Aggregate(_, exprs, _) =>
        processAliases(exprs, inputs)

      case _ =>
    }

    // Check whether plan is a relation, and whether that relation contains any of the fields
    // used to generate the query output. If so, record it as one of the inputs.
    findRelations(plan, spark).foreach { r =>
      logDebug(s"found relation: $r")
      val hasInput = r.fields.exists(inputs.isSourceField)
      if (hasInput) {
        inputs.relations(r.source) = r
      }
    }

    logDebug(s"""current lineage info:
      |  relations: ${inputs.relations}
      |  mappings: ${inputs.fieldMappings}""".stripMargin)

    plan.children.foreach { c => collectLineageInfo(c, inputs, spark) }
  }

  private def processAliases(exprs: Seq[NamedExpression], inputs: LineageInputs): Unit = {
    exprs.foreach {
      case a: Alias =>
        val sources = flattenReferences(a)
        inputs.replace(a, flattenReferences(a))

      case _ =>
    }
  }

  /**
   * Process an expression recursively looking for fields that are used to construct it (traversing
   * aliases, function calls, etc), and returns a list of input expressions.
   */
  private def flattenReferences(expr: Expression): Seq[NamedExpression] = {
    if (expr.children.nonEmpty) {
      expr.children.flatMap(flattenReferences)
    } else {
      if (expr.isInstanceOf[NamedExpression]) Seq(expr.asInstanceOf[NamedExpression]) else Nil
    }
  }

  /**
   * Returns [[SQLRelationInfo]] instances for the relation in the input plan. Normally there will
   * be a single relation, but data sources can generate multiple inputs by using multiple input
   * paths
   */
  private def findRelations(plan: LogicalPlan, spark: SparkSession): Seq[SQLRelationInfo] = {
    plan match {
      case LogicalRelation(base, fields, maybeTable) =>
        maybeTable.map { table =>
          getRelationInfo(table.identifier, spark, fields).toSeq
        }.getOrElse {
          base match {
            case HadoopFsRelation(location, _, _, _, format, _) if location.rootPaths.nonEmpty =>
              val dsFormat = maybeTable.map(getDataSourceFormat).getOrElse(
                getDataSourceFormat(format))
              location.rootPaths.map { path =>
                val dsType = getDataSourceType(path.toUri())
                SQLRelationInfo(path.toString(), fields, dsType, dsFormat)
              }.toSeq

            case _ => Nil
          }
        }

      case HiveTableRelation(table, cols, parts) =>
        getRelationInfo(table.identifier, spark, cols ++ parts).toSeq

      case _ =>
        logDebug(s"Unmatched relation:\n$plan")
        Nil
    }
  }

  /**
   * Get up-to-date information about a catalog relation referenced in a plan. In CDH, "catalog
   * relation" means a table recorded in a Hive metastore most of the time, but the code below
   * should work with an in-memory catalog also.
   */
  private def getRelationInfo(
      tid: TableIdentifier,
      spark: SparkSession,
      fields: Seq[NamedExpression]): Option[SQLRelationInfo] = {
    val name = tid.database.getOrElse(spark.catalog.currentDatabase) + "." + tid.table

    // Fetch the table from the catalog. The data in the plan may not have all the updated
    // information after the table was created, like its location.
    //
    // We can't use the public API because it doesn't contain all the info, so poke into
    // the internal session state.
    val catalogTable = spark.sessionState.catalog.getTableMetadata(tid)
    logDebug(s"Metastore table info: $catalogTable")

    // Spark's saveAsTable() API may or may not create Hive-compatible tables. People shouldn't be
    // using that API to write to the HMS, but they do. There isn't a reliable way to differentiate
    // both cases currently; this code uses a hack based on the behavior of HiveExternalCatalog. It
    // considers the table to be Hive-compatible if its serde is not
    // "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe".
    val badSerDe = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    val isHive = catalogTable.provider == Some(DDLUtils.HIVE_PROVIDER) ||
      (catalogTable.storage.serde.isDefined && catalogTable.storage.serde != badSerDe)

    val dsFormat = getDataSourceFormat(catalogTable)
    if (isHive) {
      Some(SQLRelationInfo(name, fields, DataSourceType.HIVE, dsFormat))
    } else {
      val path = catalogTable.storage.locationUri
        .orElse(catalogTable.properties.get("path").map(new URI(_)))
      path match {
        case Some(p) =>
          Some(SQLRelationInfo(p.toString(), fields, getDataSourceType(p), dsFormat))

        case None =>
          logDebug(s"Table $name has no path information, ignoring.")
          None
      }
    }
  }

  private def getDataSourceType(uri: URI): DataSourceType = {
    uri.getScheme() match {
      case "s3" => DataSourceType.S3
      case "hdfs" => DataSourceType.HDFS
      case "file" => DataSourceType.LOCAL
      case _ => DataSourceType.UNKNOWN
    }
  }

  private def getDataSourceFormat(format: FileFormat): DataSourceFormat = format match {
    case _: JsonFileFormat => DataSourceFormat.JSON
    case _: CSVFileFormat => DataSourceFormat.CSV
    case _: ParquetFileFormat => DataSourceFormat.PARQUET
    case _ => DataSourceFormat.UNKNOWN
  }

  private def getDataSourceFormat(table: CatalogTable): DataSourceFormat = {
    table.provider.orNull match {
      case "json" => DataSourceFormat.JSON
      case "csv" => DataSourceFormat.CSV
      case "parquet" => DataSourceFormat.PARQUET
      case "hive" =>
        val tableSerde = HiveSerDe(table.storage.inputFormat, table.storage.outputFormat,
          table.storage.serde)
        hiveSerdesMapping
          .collectFirst { case (serde, format) if serde == tableSerde => format }
          .getOrElse(DataSourceFormat.UNKNOWN)
      case _ => DataSourceFormat.UNKNOWN
    }
  }

  private case class SQLRelationInfo(
      source: String,
      fields: Seq[NamedExpression],
      dsType: DataSourceType,
      dsFormat: DataSourceFormat)

  /**
   * Class used internally to collect information about a query plan. It's mutable, and
   * collects information that later will be converted into an immutable LineageInfo
   * instance.
   *
   * Field mappings use [[NamedExpression]]s, which are unique, instead of the raw field
   * name, which can be ambiguous.
   */
  private class LineageInputs {
    // Maps relation names to their information.
    val relations = mutable.HashMap[String, SQLRelationInfo]()

    // Maps fields to the input fields used to generate them.
    val fieldMappings = mutable.HashMap[ExprId, Seq[NamedExpression]]()

    def isSourceField(f: NamedExpression): Boolean = {
      fieldMappings.exists { case (_, sources) => sources.exists(f.semanticEquals) }
    }

    def replace(e: NamedExpression, sources: Seq[NamedExpression]): Unit = {
      val updated = fieldMappings.toSeq.collect {
        case (out, _) if out == e.exprId =>
          (out, sources)

        case (out, old) if old.exists { f => f.exprId == e.exprId } =>
          val newSources = sources.filterNot { f => f.exprId == e.exprId } ++ sources
          (out, newSources)
      }.toMap
      fieldMappings ++= updated
    }

  }

}
