package com.joom

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, MillisToTs}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions.{col, concat, hash, lit, struct, when}
import org.apache.spark.sql.types.{DataType, StringType, StructType, TimestampType}

import java.io.IOException

package object spark {

  /** Convert long millisecond-since-epoch value into Timestamp type */
  def millis_to_ts(c: Column): Column = (c / 1000).cast(TimestampType)

  object implicits {

    implicit class DeduplicateWithinPartitionsWrapper(df: DataFrame) {
      def deduplicateWithinPartitions(columnNames: Seq[String]): DataFrame = {
        deduplicateWithinPartitions(columnNames, columnNames)
      }
      def deduplicateWithinPartitions(sortColumnNames: Seq[String], comparisonColumnNames: Seq[String]): DataFrame = {
        val sparkSession = df.sparkSession
        val schema = df.schema
        val resolver = sparkSession.sessionState.analyzer.resolver
        val allColumns = df.queryExecution.analyzed.output
        def resolveColumn(colName: String) = {
          // It is possibly there are more than one columns with the same name,
          // so we call filter instead of find.
          val cols = allColumns.filter(col => resolver(col.name, colName))
          if (cols.isEmpty) {
            throw new Exception(
              s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
          }
          cols
        }
        val sortColumns = sortColumnNames.flatMap { resolveColumn(_) }
        val comparisonColumns = comparisonColumnNames.flatMap { resolveColumn(_) }
        internal.dataFrame(sparkSession,
          DeduplicateWithinPartitions(sortColumns, comparisonColumns, df.queryExecution.analyzed))
      }
    }

    implicit class ExplicitRepartitionWrapper(df: DataFrame) {
      def explicitRepartition(numPartitions: Int, partitionExpression: Column): DataFrame = {
        val sparkSession = df.sparkSession
        internal.dataFrame(sparkSession,
          ExplicitRepartition(internal.columnExpression(partitionExpression, sparkSession), df.queryExecution.analyzed, numPartitions))
      }
    }

    implicit class DropNestedColumnWrapper(df: DataFrame) {
      def dropNestedColumn(colName: String): DataFrame = {
        df.schema.fields
          .flatMap(f => {
            if (colName.startsWith(s"${f.name}.")) {
              dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
                case Some(x) => Some((f.name, x))
                case None => None
              }
            } else {
              None
            }
          })
          .foldLeft(df.drop(colName)) {
            case (df, (colName, column)) => df.withColumn(colName, column)
          }
      }

      private def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
        if (fullColName.equals(dropColName)) {
          None
        } else {
          colType match {
            case colType: StructType =>
              if (dropColName.startsWith(s"$fullColName.")) {
                Some(struct(
                  colType.fields
                    .flatMap(f =>
                      dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                        case Some(x) => Some(x.alias(f.name))
                        case None => None
                      })
                    : _*))
              } else {
                Some(col)
              }
            case _ => Some(col)
          }
        }
      }
    }

    implicit class DatasetWrapper(ds: Dataset[_]) {

      def joinAvoidSkewDueToNulls(right: Dataset[_], leftJoinCol: String, rightJoinCol: String, joinType: String): DataFrame = {
        val inputColumnNames = ds.columns ++ right.columns
        val joinColName = Iterator.iterate("_joinColName")(_ + "_")
          .find(name => !inputColumnNames.exists(_.equalsIgnoreCase(name))).get

        // Input to function hash cannot contain elements of MapType, since in Spark, same maps may have different hashcode.
        def colsWithoutMaps(ds: Dataset[_]) = ds.schema.fields
          .filterNot(_.dataType.catalogString.contains("map<"))
          .map(f => ds(s"`${f.name.replace("`", "``")}`"))

        // Analyze scalar equality, without executing it, to reuse Spark's own
        // ANSI, decimal, date and collation coercion before wrapping the keys.
        val leftKey = ds.select(ds(leftJoinCol).as("_leftKey"))
        val rightKey = right.select(right(rightJoinCol).as("_rightKey"))
        val comparison = leftKey.crossJoin(rightKey)
          .select((col("_leftKey") === col("_rightKey")).as("_comparison"))
          .queryExecution.analyzed.asInstanceOf[Project].projectList.head
        val (leftType, rightType) = comparison match {
          case Alias(EqualTo(left, right), _) => (left.dataType, right.dataType)
          case other => throw new IllegalStateException(s"Unexpected analyzed key equality: $other")
        }

        def withJoinKey(input: Dataset[_], name: String, prefix: String, keyType: DataType) = {
          val key = input(name).cast(keyType)
          val salt = when(key.isNull,
            concat(lit(prefix), hash(colsWithoutMaps(input): _*).cast(StringType)))
            .otherwise(lit(null).cast(StringType))
          input.withColumn(joinColName, struct(key.as("value"), salt.as("nullSalt")))
        }

        val dsEx = withJoinKey(ds, leftJoinCol, "left", leftType)
        val rightEx = withJoinKey(right, rightJoinCol, "right", rightType)

        // One composite key keeps the salt in the shuffle even when Spark can
        // prove that the other input's original key is never null.
        dsEx.join(rightEx, Seq(joinColName), joinType)
          .drop(joinColName)
      }
    }
  }

  // Extra optimization strategies needed to make our extensions work.
  def extraStrategies = Seq(DeduplicateWithinPartitionsStrategy, ExplicitRepartitionStrategy)

  def using[T <: {def close(): Unit}, R](resource: T)(block: T => R): R = {
    try {
      block(resource)
    } finally {
      if (resource != null) {
        try {
          resource.close()
        } catch {
          case e: IOException => println(s"Cannot close resource. $e")
        }
      }
    }
  }

}
