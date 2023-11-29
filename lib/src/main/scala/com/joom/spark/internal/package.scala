package com.joom.spark

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types.StructType

import java.lang.reflect.InvocationTargetException

package object internal {

  def rowEncoderSpark35(schema: StructType): Encoder[Row] = {
    val clazz = Class.forName("org.apache.spark.sql.Encoders$")
    val method = clazz.getDeclaredMethod("row", classOf[StructType])
    val field = clazz.getField("MODULE$")
    val instance = field.get(null)
    method.invoke(instance, schema).asInstanceOf[Encoder[Row]]
  }

  def rowEncoderSpark34(schema: StructType): Encoder[Row] = {
    val clazz = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder$")
    val method = clazz.getDeclaredMethod("apply", classOf[StructType])
    val field = clazz.getField("MODULE$")
    val instance = field.get(null)
    method.invoke(instance, schema).asInstanceOf[Encoder[Row]]
  }

  def rowEncoder(schema: StructType): Encoder[Row] = {
    try {
      rowEncoderSpark35(schema)
    } catch {
      case e@(_: ClassNotFoundException | _: InstantiationException | _: IllegalAccessException | _: NoSuchMethodException | _: SecurityException | _: InvocationTargetException) =>
        rowEncoderSpark34(schema)
    }
  }
}
