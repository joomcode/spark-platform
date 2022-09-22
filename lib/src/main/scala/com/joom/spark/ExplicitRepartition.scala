
package com.joom.spark

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionOperation}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_NUM, ShuffleExchangeExec}

// This extension allows to repartition a dataset with expression that gives
// exact partition number for a row. Normally, a repartition takes hash
// of the expression(s), however

// As ugly as it is, we need to inherit from HashPartitioning, because
// ShuffleExchangeExec.explicitRepartition hardcodes the lists of partitionings it
// supports.
class ExplicitPartitioning(expression2: Seq[Expression], numPartitions2: Int) extends HashPartitioning(expression2, numPartitions2) {
  require(expression2.size == 1, s"Number of expression2 ($numPartitions) must be 1.")

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required)
  }

  override def partitionIdExpression: Expression = expression2.head
}

case class ExplicitRepartition(
                                partitionExpression: Expression,
                                child: LogicalPlan,
                                numPartitions: Int) extends RepartitionOperation {

  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")

  val partitioning: Partitioning = {
    new ExplicitPartitioning(Seq(partitionExpression), numPartitions)
  }

  override def shuffle: Boolean = true

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
}

object ExplicitRepartitionStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case r: com.joom.spark.ExplicitRepartition =>
      ShuffleExchangeExec(r.partitioning, planLater(r.child), REPARTITION_BY_NUM) :: Nil
    case _ => Nil
  }
}
