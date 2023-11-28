package com.joom.spark

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types.StructType

package object internal {
  def rowEncoder(schema: StructType): Encoder[Row] = RowEncoder(schema)
}
