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
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect, LogicalRelation}
import org.apache.spark.sql.hive.{InsertIntoHiveTable, MetastoreRelation}
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
object QueryAnalysis extends Logging {

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
          case InsertIntoHiveTable(m, _, _, _, _) =>
            Some(QueryDetails(m.databaseName + "." + m.tableName,
                m.output.map(_.name).to[ListBuffer], DataSourceType.HIVE))
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
    val tlas = getTopLevelAttributes(qe)
    logDebug(s"Top level attributes = $tlas")
    tlas.foldLeft(List.empty[FieldDetails]) { (acc, a) =>
      getRelation(qe.optimizedPlan, a) match {
        case Some(rel) =>
          logDebug(s"relation for attribute($a) is $rel")
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
        case None =>
          logDebug(s"no root relation found for attribute $a")
          acc
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

  /**
   * Returns back if the plan has an aggregate function
   * @param plan - the plan to be analyzed
   */
  def hasAggregateFunction(plan: LogicalPlan): Boolean = {
    plan match {
      case agg: Aggregate => true
      case lp: LogicalPlan => lp.children.exists(hasAggregateFunction)
    }
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
    val namedExpressions = getTopLevelNamedExpressions(plan)
    logDebug(s"namedExpressions: ${namedExpressions}")
    val res = namedExpressions.map(getAttributesReferences)
    res
  }

  private def getTopLevelNamedExpressions(
      plan: LogicalPlan,
      output: Boolean = false): Option[List[NamedExpression]] = {
    logDebug(s"plan is: ${plan}")
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
    resolveAttributeReferences(seqExpression, List.empty[AttributeReference])
  }

  private def resolveAttributeReferences(
      seqExpression: Seq[Expression],
      runningList: List[AttributeReference]): List[AttributeReference] = {
    seqExpression.foldLeft(runningList) { (acc, node) =>
      logDebug(s"Calling resolveAttributeReferences on ${node}")
      node.children match {
        case Nil =>
          logDebug("resolveAttributeReferences: no children found")
          node match {
            case ar: AttributeReference =>
              ar :: acc
            case _ => acc
          }
        case _ =>
          resolveAttributeReferences(node.children, acc)
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
    logDebug(s"trying to get relation for ${attr.simpleString} in:\n$plan")
    plan match {
      case r: LogicalRelation =>
        if (getAttributesReferences(r.output).exists(a => a.sameRef(attr))) {
          Some(r)
        } else {
          logDebug("Unable to find a matching LogicalRelation.")
          None
        }
      case m: MetastoreRelation => Some(m)
      case CreateTableAsSelect(_, query, _) => getRelation(query, attr)
      case InsertIntoHiveTable(_, _, child, _, _) => getRelation(child, attr)
      case p @ Project(_, _) =>
        val attrRefs = getAttributesReferences(p.projectList)
        val childRefsAttr = attrRefs.exists(a => a.sameRef(attr))
        logDebug(s"project w/ attrRefs = $attrRefs; checking for $attr; $childRefsAttr")
        if (childRefsAttr) {
          getRelation(p.child, attr)
        } else {
          logDebug("Unable to find a matching Project.")
          None
        }
      case binaryNode: BinaryNode => getRelation(binaryNode.children(0), attr).orElse(
          getRelation(binaryNode.children(1), attr))
      case agg: Aggregate =>
        // For each of the top level attributes (i.e. elements in select clause), the logical
        // plan is recursively traversed. That means this case is hit for every single attribute
        // in the select clause.
        // Aggregation can be present in one of two ways today - #1 at the top level in which all
        // works well, or #2 in a sub-query in which case it appears as avg(my_col) instead of
        // my_col in lineage. So, for #2, the question is what table should avg(my_col) be shown
        // from. The answer in such a case is, of course, the table which my_col is from. But what
        // if there was another UDAF called my_udaf that took two parameters -
        // my_udaf(table1.my_col1, table2.my_col2) - that's of course a join query with an
        // aggregation on the output of the join results. In such a case, we may ask, what table
        // should my_udaf(table1.my_col1, table2.my_col2) be tied to, given the current state.
        // And, this piece of code  below ties the said state, arbitrarily to the table where the
        // first column comes from.
        // The way to fix that long term is CDH-57411, it's to allow recursive traversal to add more
        // elements to the "input" in lineage json. Currently you can only start with the top level
        // attributes.
        //
        // For #1, this case isn't all that relevant because if an aggregation function is included
        // in the top level attributes, we resolve the attributes in the top level itself -
        // so covar(col1, col2) turns into col1, col2 right at the beginning.
        // So, none of matching is relevant for that.
        //
        val aggOption = agg.aggregateExpressions.collectFirst{
          case aggExp if (aggExp.exprId == attr.exprId) =>
            if (aggExp.references.isEmpty) {
              logDebug(s"Found an aggregate with no references. agg = ${agg}")
              None
            } else {
              getRelation(agg.child, aggExp.references.head.asInstanceOf[AttributeReference])
            }
        }
        // aggOption is none when the top level attribute for which lineage is being generated
        // is non in aggregateExpressions.
        // For a query like "select city, sum(income) from foo group by city",
        // agg.aggregateExpression is city. So when lineage is being generated for income column
        // aggOption will be none.
        aggOption.getOrElse(getRelation(agg.child, attr))
      case unaryNode: UnaryNode =>
        getRelation(unaryNode.child, attr)
      case _ =>
        logDebug(s"unhandled plan node: $plan")
        None
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
