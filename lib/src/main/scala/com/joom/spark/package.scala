package com.joom

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.MillisToTs
import org.apache.spark.sql.functions.{coalesce, col, hash, lit, struct, concat}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.IOException

package object spark {

  /** Convert long millisecond-since-epoch value into Timestamp type */
  def millis_to_ts(c: Column) = new Column(MillisToTs(c.expr))

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
        val e = RowEncoder(df.schema)
        new DataFrame(sparkSession, DeduplicateWithinPartitions(sortColumns, comparisonColumns, df.queryExecution.analyzed), e)
      }
    }

    implicit class ExplicitRepartitionWrapper(df: DataFrame) {
      def explicitRepartition(numPartitions: Int, partitionExpression: Column): DataFrame = {
        val sparkSession = df.sparkSession
        val e = RowEncoder(df.schema)
        new DataFrame(sparkSession,
          ExplicitRepartition(partitionExpression.expr, df.queryExecution.analyzed, numPartitions), e)
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
        val joinColName = "_joinColName"

        // Input to function hash cannot contain elements of MapType, since in Spark, same maps may have different hashcode.
        def colsWithoutMaps(ds: Dataset[_]) = ds.schema.fields
          .filterNot(_.dataType.catalogString.contains("map<"))
          .map(f => col(f.name))

        val dsEx = ds.withColumn(joinColName, coalesce(ds(leftJoinCol), concat(lit("left"), hash(colsWithoutMaps(ds): _*))))
        val rightEx = right.withColumn(joinColName, coalesce(right(rightJoinCol), concat(lit("right"), hash(colsWithoutMaps(right): _*))))

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
