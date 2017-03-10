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

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.fasterxml.jackson.core.`type`.TypeReference

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.{
  CreateTableUsing,
  CreateTableUsingAsSelect,
  LogicalRelation
}
import org.apache.spark.sql.hive.MetastoreRelation
import org.apache.spark.sql.hive.execution.CreateTableAsSelect
import org.apache.spark.sql.query.analysis.DataSourceFormat.DataSourceFormat
import org.apache.spark.sql.query.analysis.DataSourceType.DataSourceType
import org.apache.spark.sql.sources.HadoopFsRelation

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This class is responsible for analyzing the {@link QueryExecution} and extracting the input
 * and output metadata
 */
object QueryAnalysis {

  def getOutputMetaData(qe: QueryExecution): Option[QueryDetails] = {
    getTopLevelNamedExpressions(qe.optimizedPlan, true) match {
      case Some(s) => getSource(qe, qe.outputParams, s.map(_.name))
      case None => None
    }
  }

  private def getSource(
      qe: QueryExecution,
      extraParams: Map[String, String],
      fields: List[String]): Option[QueryDetails] = {
    extraParams.get("path") match {
      case Some(path) =>
        val qualifiedPath = getQualifiedFilePath(qe.sqlContext.sparkContext, path)
        Some(QueryDetails(qualifiedPath, fields.to[ListBuffer], getDataSourceType(qualifiedPath)))
      case None => {
        qe.optimizedPlan match {
          case CreateTableUsing(t, _, _, _, _, _, _) =>
            Some(QueryDetails(getQualifiedDBName(qe, t), fields.to[ListBuffer],
                DataSourceType.HIVE))
          case CreateTableUsingAsSelect(t, _, _, _, _, _, _) =>
            Some(QueryDetails(getQualifiedDBName(qe, t), fields.to[ListBuffer],
                DataSourceType.HIVE))
          case CreateTableAsSelect(ht, _, _) =>
            Some(QueryDetails(ht.database + "." + ht.name, fields.to[ListBuffer],
                DataSourceType.HIVE))
          case _ => None
        }
      }
    }
  }

  private def getQualifiedDBName(qe: QueryExecution, tableIdentifier: TableIdentifier): String = {
    tableIdentifier.database.getOrElse(qe.sqlContext.catalog.getCurrentDatabase) + "." +
      tableIdentifier.table
  }

  private def getQualifiedFilePath(sparkContext: SparkContext, path: String): String = {
    val filePath = new Path(path)
    val fs = filePath.getFileSystem(sparkContext.hadoopConfiguration)
    filePath.makeQualified(fs.getUri, filePath).toString
  }

  private def getDataSourceType(path: String): DataSourceType = {
    path match {
      case p if p.startsWith("s3") => DataSourceType.S3
      case p if p.startsWith("hdfs") => DataSourceType.HDFS
      case p if p.startsWith("file") => DataSourceType.LOCAL
      case _ => DataSourceType.UNKNOWN
    }
  }

  /**
   * Extracts the input metadata from the @see [[QueryExecution]] object.
   *
   * @param qe
   * @return
   */
  def getInputMetadata(qe: QueryExecution): List[FieldDetails] = {
    getTopLevelAttributes(qe).foldLeft(List.empty[FieldDetails]) { (acc, a) =>
      getRelation(qe.optimizedPlan, a) match {
        case Some(rel) =>
          rel match {
            case LogicalRelation(_, _, Some(tableId)) =>
              FieldDetails(Array(tableId.unquotedString), a.name, DataSourceType.HIVE) :: acc
            case LogicalRelation(p: ParquetRelation, _, _)
              if p.parameters.contains(ParquetRelation.METASTORE_TABLE_NAME) =>
                FieldDetails(Array(p.parameters.get(ParquetRelation.METASTORE_TABLE_NAME).get),
                  a.name, DataSourceType.HIVE) :: acc
            case LogicalRelation(dfsRel: HadoopFsRelation, _, _) =>
              val paths = dfsRel.paths
              FieldDetails(paths, a.name, getDataSourceType(paths(0))) :: acc
            case m: MetastoreRelation =>
              FieldDetails(Array(m.databaseName + "." + m.tableName), a.name,
                DataSourceType.HIVE) :: acc
            case _ => acc
          }
        case None => acc
      }
    }
  }

  /**
   * Converts the list of input metadata into a map of format [table -> fields read from table]
   *
   * @param list
   * @return
   */
  def convertToQueryDetails(list: List[FieldDetails]): Iterable[QueryDetails] = {
    val map = list.foldLeft(mutable.Map.empty[String, QueryDetails]) { (map, fieldDetails) =>
      fieldDetails.source.foreach { s =>
        val queryDetails = map.getOrElseUpdate(s,
          QueryDetails(s, new ListBuffer[String], fieldDetails.sourceType))
        queryDetails.fields.append(fieldDetails.field)
      }
      map
    }
    map.values
  }

  private def getTopLevelAttributes(qe: QueryExecution): List[AttributeReference] = {
    getTopLevelAttributes(qe.optimizedPlan).getOrElse(List.empty)
  }

  /**
   * Extract the {@link AttributeReference}s from the {@link LogicalPlan} that will be written out
   * when the plan is executed. These are the first element of either {@link Project} or
   * {@link LogicalRelation}
   */
  private def getTopLevelAttributes(plan: LogicalPlan): Option[List[AttributeReference]] = {
    getTopLevelNamedExpressions(plan).map(getAttributesReferences)
  }

  private def getTopLevelNamedExpressions(
      plan: LogicalPlan,
      output: Boolean = false): Option[List[NamedExpression]] = {
    plan.collectFirst {
      case p @ Project(_, _) => p.projectList.toList
      case Join(left, right, _, _) => {
        val leftTop = getTopLevelNamedExpressions(left, output).getOrElse(List.empty)
        val rightTop = getTopLevelNamedExpressions(right, output).getOrElse(List.empty)
        leftTop ::: rightTop
      }
      case l: LocalRelation if output => l.output.toList
      case l: LogicalRelation => l.output.toList
      case m: MetastoreRelation => m.attributes.toList
    }
  }

  /**
   * Given a sequence of NamedExpressions which could contain AttributeReferences as well as
   * Aliases return back a list of AttributeReferences
   */
  private def getAttributesReferences(
      seqExpression: Seq[NamedExpression]): List[AttributeReference] = {
    seqExpression.foldLeft(List.empty[AttributeReference]) { (acc, node) =>
        node match {
          case ar: AttributeReference => ar :: acc
          case Alias(child: AttributeReference, _) => child :: acc
          case _ => acc
      }
    }
  }

  /**
   * Extract the logical or metastore relation associated with this particular attribute.
   * Essentially given an attribute like a column from a hive table or a column from a json file
   * it will return back the relation representing the hive table or json file
   *
   * @param plan - The root of the plan from where to begin the analysis
   * @param attr - The attribute to dereference
   * @return An Option representing a LeafNode
   */
  private def getRelation(plan: LogicalPlan, attr: AttributeReference): Option[LeafNode] = {
    plan match {
      case r: LogicalRelation =>
        if (getAttributesReferences(r.output).exists(a => a.sameRef(attr))) {
          Some(r)
        } else {
          None
        }
      case m: MetastoreRelation => Some(m)
      case CreateTableAsSelect(_, query, _) => getRelation(query, attr)
      case p @ Project(_, _) =>
        if (getAttributesReferences(p.projectList).exists(a => a.sameRef(attr))) {
          getRelation(p.child, attr)
        } else {
          None
        }
      case binaryNode: BinaryNode => getRelation(binaryNode.children(0), attr).orElse(
          getRelation(binaryNode.children(1), attr))
      case unaryNode: UnaryNode => getRelation(unaryNode.children(0), attr)
      case _ => None
    }
  }
}

case class QueryDetails(
    source: String,
    fields: ListBuffer[String],
    @JsonScalaEnumeration(classOf[DataSourceTypeType]) dataSourceType: DataSourceType =
      DataSourceType.UNKNOWN,
    @JsonScalaEnumeration(classOf[DataSourceFormatType]) dataSourceFormat: DataSourceFormat =
      DataSourceFormat.UNKNOWN) {
  var dataQuery: String = _
  var hiveMetastoreLocation: Option[String] = _
}

case class FieldDetails(
    source: Seq[String],
    field: String,
    sourceType: DataSourceType = DataSourceType.UNKNOWN,
    format: DataSourceFormat = DataSourceFormat.UNKNOWN)

object DataSourceType extends Enumeration {
  type DataSourceType = Value
  val HIVE, HDFS, S3, LOCAL, UNKNOWN = Value
}

object DataSourceFormat extends Enumeration {
  type DataSourceFormat = Value
  val JSON, CSV, PARQUET, AVRO, UNKNOWN = Value
}

class DataSourceTypeType extends TypeReference[DataSourceType.type]
class DataSourceFormatType extends TypeReference[DataSourceFormat.type]
