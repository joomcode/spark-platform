package com.joom

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.MillisToTs

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
  }

}
