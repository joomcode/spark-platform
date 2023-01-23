
package com.joom.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/** This module adds 'deduplicateWithinPartition' low-level method for deduplicating data.
  * The standard `dropDuplicates` method becomes `SortAggregateExec` and it causes
  * repartioning of the data by all the keys. Support we have data partitioned by
  * device_id, and we want to deduplicate by (device_id, event_id). Then, we can just
  * sort each partition by (device_id, event_id), and drop duplicates by linear scan, which is what
  * this extension does.
  */

  case class DeduplicateWithinPartitions(sortKeys: Seq[Attribute], comparisonKeys: Seq[Attribute], child: LogicalPlan)
  extends UnaryNode {
    override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
}

  case class DeduplicateWithinPartitionsExec(sortKeys: Seq[Attribute], comparisonKeys: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

    private val ordering = sortKeys.map(SortOrder(_, Ascending))

    override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
      ordering :: Nil
    }

    override def outputPartitioning: Partitioning = child.outputPartitioning

    override def outputOrdering: Seq[SortOrder] = ordering

    override protected def doExecute(): RDD[InternalRow] = {
      val filter = (it: Iterator[InternalRow]) => {

        val keysProjection = UnsafeProjection.create(comparisonKeys, child.output)
        new Iterator[InternalRow] {
          var value: Option[InternalRow] = if (it.hasNext) Some(it.next().copy()) else None
          override def hasNext: Boolean = value.isDefined

          override def next(): InternalRow = {
            val r = value.get
            val keyValues = keysProjection(r).copy()
            value = None
            while (it.hasNext && value.isEmpty) {
              val nextRow = it.next()
              val nextKeys = keysProjection(nextRow)
              if (keyValues != nextKeys) {
                value = Some(nextRow.copy())
              }
            }
            r
          }
        }
      }
      child.execute().mapPartitions(filter, false)
    }

    override def output: Seq[Attribute] = child.output

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
  }

  object DeduplicateWithinPartitionsStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case DeduplicateWithinPartitions(sortKeys, comparisonKeys, child) =>
        DeduplicateWithinPartitionsExec(sortKeys, comparisonKeys, planLater(child)) :: Nil
      case _ => Nil
    }
  }

