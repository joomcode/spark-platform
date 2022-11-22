package com.joom

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.io.IOException

package object spark {

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
