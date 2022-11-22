package com.joom

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.MillisToTs

package object spark {

  /** Convert long millisecond-since-epoch value into Timestamp type */
  def millis_to_ts(c: Column) = new Column(MillisToTs(c.expr))

  object implicits {
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
