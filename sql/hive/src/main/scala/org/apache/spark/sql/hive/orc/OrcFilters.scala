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

package org.apache.spark.sql.hive.orc

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentFactory}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper object for building ORC `SearchArgument`s, which are used for ORC predicate push-down.
 *
 * Due to limitation of ORC `SearchArgument` builder, we had to end up with a pretty weird double-
 * checking pattern when converting `And`/`Or`/`Not` filters.
 *
 * An ORC `SearchArgument` must be built in one pass using a single builder.  For example, you can't
 * build `a = 1` and `b = 2` first, and then combine them into `a = 1 AND b = 2`.  This is quite
 * different from the cases in Spark SQL or Parquet, where complex filters can be easily built using
 * existing simpler ones.
 *
 * The annoying part is that, `SearchArgument` builder methods like `startAnd()`, `startOr()`, and
 * `startNot()` mutate internal state of the builder instance.  This forces us to translate all
 * convertible filters with a single builder instance. However, before actually converting a filter,
 * we've no idea whether it can be recognized by ORC or not. Thus, when an inconvertible filter is
 * found, we may already end up with a builder whose internal state is inconsistent.
 *
 * For example, to convert an `And` filter with builder `b`, we call `b.startAnd()` first, and then
 * try to convert its children.  Say we convert `left` child successfully, but find that `right`
 * child is inconvertible.  Alas, `b.startAnd()` call can't be rolled back, and `b` is inconsistent
 * now.
 *
 * The workaround employed here is that, for `And`/`Or`/`Not`, we first try to convert their
 * children with brand new builders, and only do the actual conversion with the right builder
 * instance when the children are proven to be convertible.
 *
 * P.S.: Hive seems to use `SearchArgument` together with `ExprNodeGenericFuncDesc` only.  Usage of
 * builder methods mentioned above can only be found in test code, where all tested filters are
 * known to be convertible.
 */
private[orc] object OrcFilters extends Logging {
  def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap

    // First, tries to convert each filter individually to see whether it's convertible, and then
    // collect all convertible ones to build the final `SearchArgument`.
    val convertibleFilters = for {
      filter <- filters
      _ <- buildSearchArgument(dataTypeMap, filter, SearchArgumentFactory.newBuilder())
    } yield filter

    for {
      // Combines all convertible filters using `And` to produce a single conjunction
      conjunction <- convertibleFilters.reduceOption(And)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(dataTypeMap, conjunction, SearchArgumentFactory.newBuilder())
    } yield builder.build()
  }

  private def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    def newBuilder = SearchArgumentFactory.newBuilder()

    def predicateLeafType(dataType: DataType): Option[PredicateLeaf.Type] = dataType match {
      case ByteType | ShortType | IntegerType | LongType => Some(PredicateLeaf.Type.LONG)
      case FloatType | DoubleType => Some(PredicateLeaf.Type.FLOAT)
      case StringType => Some(PredicateLeaf.Type.STRING)
      case TimestampType => Some(PredicateLeaf.Type.TIMESTAMP)
      case _: DecimalType => Some(PredicateLeaf.Type.DECIMAL)
      case BooleanType => Some(PredicateLeaf.Type.BOOLEAN)
      case _ => None
    }

    def coerce(value: Any, ltype: PredicateLeaf.Type): Any = ltype match {
      case PredicateLeaf.Type.LONG => value.asInstanceOf[Number].longValue()
      case PredicateLeaf.Type.FLOAT => value.asInstanceOf[Number].doubleValue()
      case PredicateLeaf.Type.DECIMAL =>
        new HiveDecimalWritable(HiveDecimal.create(value.asInstanceOf[java.math.BigDecimal]))
      case _ => value
    }

    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        // Pushing one side of AND down is only safe to do at the top level.
        // You can see ParquetRelation's initializeLocalJobFunc method as an example.
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startAnd())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Or(left, right) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startOr())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, child, newBuilder)
          negate <- buildSearchArgument(dataTypeMap, child, builder.startNot())
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startAnd().equals(attribute, ltype, coerce(value, ltype)).end()
        }

      case EqualNullSafe(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startAnd().nullSafeEquals(attribute, ltype, coerce(value, ltype)).end()
        }

      case LessThan(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startAnd().lessThan(attribute, ltype, coerce(value, ltype)).end()
        }

      case LessThanOrEqual(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startAnd().lessThanEquals(attribute, ltype, coerce(value, ltype)).end()
        }

      case GreaterThan(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startNot().lessThanEquals(attribute, ltype, coerce(value, ltype)).end()
        }

      case GreaterThanOrEqual(attribute, value) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startNot().lessThan(attribute, ltype, coerce(value, ltype)).end()
        }

      case IsNull(attribute) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startAnd().isNull(attribute, ltype).end()
        }

      case IsNotNull(attribute) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          builder.startNot().isNull(attribute, ltype).end()
        }

      case In(attribute, values) =>
        predicateLeafType(dataTypeMap(attribute)).map { ltype =>
          val coerced = values.map { v => coerce(v, ltype).asInstanceOf[AnyRef] }
          builder.startAnd().in(attribute, ltype, coerced: _*).end()
        }

      case _ => None
    }
  }
}
