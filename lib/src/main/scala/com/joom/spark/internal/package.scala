package com.joom.spark

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import java.lang.reflect.InvocationTargetException

package object internal {

  // Keep Spark's actionable exceptions visible to callers of the compatibility
  // adapters instead of exposing the reflection transport exception.
  private def invokeSpark[T](operation: => T): T = {
    try operation catch {
      case error: InvocationTargetException => throw error.getCause
    }
  }

  // Spark 4 moved the concrete Dataset/SparkSession to sql.classic. Keep the
  // same shared-source compatibility approach as the row-encoder adapter below.
  private lazy val dataFrameConstructor = {
    val (datasetClass, sessionClass) = try {
      (Class.forName("org.apache.spark.sql.classic.Dataset"),
        Class.forName("org.apache.spark.sql.classic.SparkSession"))
    } catch {
      case _: ClassNotFoundException => (classOf[Dataset[_]], classOf[SparkSession])
    }
    datasetClass.getConstructor(sessionClass, classOf[LogicalPlan], classOf[Encoder[_]])
  }

  def dataFrame(session: SparkSession, plan: LogicalPlan): DataFrame = {
    invokeSpark(dataFrameConstructor.newInstance(session, plan, rowEncoder(plan.schema)).asInstanceOf[DataFrame])
  }

  def columnExpression(column: Column, session: SparkSession): Expression = {
    try {
      invokeSpark(column.getClass.getMethod("expr").invoke(column).asInstanceOf[Expression])
    } catch {
      case _: NoSuchMethodException =>
        invokeSpark(session.getClass.getMethod("expression", classOf[Column]).invoke(session, column).asInstanceOf[Expression])
    }
  }

  def rowEncoderSpark35(schema: StructType): Encoder[Row] = {
    val clazz = Class.forName("org.apache.spark.sql.Encoders$")
    val method = clazz.getDeclaredMethod("row", classOf[StructType])
    val field = clazz.getField("MODULE$")
    val instance = field.get(null)
    invokeSpark(method.invoke(instance, schema).asInstanceOf[Encoder[Row]])
  }

  def rowEncoderSpark34(schema: StructType): Encoder[Row] = {
    val clazz = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder$")
    val method = clazz.getDeclaredMethod("apply", classOf[StructType])
    val field = clazz.getField("MODULE$")
    val instance = field.get(null)
    invokeSpark(method.invoke(instance, schema).asInstanceOf[Encoder[Row]])
  }

  def rowEncoder(schema: StructType): Encoder[Row] = {
    try {
      rowEncoderSpark35(schema)
    } catch {
      case _: ClassNotFoundException | _: InstantiationException | _: IllegalAccessException | _: NoSuchMethodException | _: SecurityException =>
        rowEncoderSpark34(schema)
    }
  }
}
