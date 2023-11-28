package com.joom.spark

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.types.StructType

package object internal {
  def rowEncoder(schema: StructType): Encoder[Row] = Encoders.row(schema)
}
