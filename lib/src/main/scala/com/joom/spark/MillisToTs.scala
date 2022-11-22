
// Yes, we need to inject ourselves into Spark to get access to AbstractDataType.
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

/** An expression that performs direct cast from Long value with milliseconds to Timestamp
 *  While internally, Catalyst represents uses 64-bit value, it's pretty awkward to produce that value from
 *  an existing milliseconds since epoch value:
 *  - Casting from Long time interprets the value as seconds since epoch
 *  - Using from_unix does the same
 *  - It might be possible to go via string date representation, but it's quite inefficient
 *  - Stackoverflow is not helpful.
 *
 * Therefore, this expression basically very direct transformation from milliseconds into internal
 * format.
 */
case class MillisToTs(millis: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = millis

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)
  override def dataType: DataType = TimestampType

  override protected def nullSafeEval(timestamp: Any): Any = {
    timestamp.asInstanceOf[Long] * 1000
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c * 1000")
  }

  override def prettyName: String = "millis_to_ts"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(millis = newChild)
}
