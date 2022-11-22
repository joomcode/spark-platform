package com.joom.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object CheckEventSequence {
  /** Given a sequence of events coming from different sources and counted within each source,
    * verify that the sequence has no duplicates and no gaps.
    *
    * Assumes that each row has 'sourceColumnName' column telling who generated this event, and the
    * normally, events with same source should have the 'counterColumnName' column start with 1 and
    * increase monitonically. Returns a dataframe that, for each source, contains count:
    * - duplicate events, that is events with the same counter as the previous one in counter order
    * - missing events, that is missing numbers between observed counters
    * - first and last event for each source.
    *
    * In order to produce most useful diagnostics, also assumes each event has 'timestampColumnName' and
    * 'typeColumnName'.
    *
    * For performance, event counters are processed in buckets of (1 << bucketSizeBits) size.
    */
  def apply(df: Dataset[Row],
    sourceColumnName: String,
    counterColumnName: String,
    typeColumnName: String,
    timestampColumnName: String,
    bucketSizeBits: Int = 20
  ) = {
    import df.sparkSession.implicits._

    val sourceColumn = col(sourceColumnName)
    val counterColumn = col(counterColumnName)
    val typeColumn = col(typeColumnName)
    val timestampColumn = col(timestampColumnName)

    val w = Window.partitionBy(sourceColumn, $"bucket").orderBy(counterColumn)
    val w2 = Window.partitionBy(sourceColumn).orderBy(col("firstEvent." + counterColumnName))

    val report = df
      // If a few number of source generate a lot of events, processing events from one source
      // in one execution is unpractical, so split events into buckets.
      .withColumn("bucket", shiftright(counterColumn, bucketSizeBits))
      // Duplicates are easy
      .withColumn("duplicate", when(counterColumn === lag(counterColumn, 1).over(w), 1).otherwise(0))
      // Compute missing elements inside bucket first. Will be undefined for the first element
      // For duplicate elements, will be 0 thanks to 'greatest' with 0
      .withColumn("missing", greatest(counterColumn - lag(counterColumn, 1).over(w) - 1, lit(0)))
      .groupBy(sourceColumn, $"bucket")
      .agg(
        count(counterColumn).as("count"),
        sum("duplicate").as("duplicate"),
        sum("missing").as("missing"),
        min(struct(counterColumn, typeColumn, timestampColumn)).as("firstEvent"),
        max(struct(counterColumn, typeColumn, timestampColumn)).as("lastEvent")
      )
      // Compute gaps between buckets of the source even source
      .withColumn("missingBetweenBuckets",
        col("firstEvent." + counterColumnName) - lag(col("lastEvent." + counterColumnName), 1).over(w2) - 1)
      .groupBy(sourceColumn)
      .agg(
        sum($"count").as("count"),
        sum("duplicate").as("duplicate"),
        (coalesce(sum($"missing"), lit(0)) + coalesce(sum($"missingBetweenBuckets"), lit(0))).as("missing"),
        min("firstEvent").as("firstEvent"),
        max("lastEvent").as("lastEvent")
      )
      .withColumn("haveDuplicate", when($"duplicate" > 0, 1).otherwise(0))
      .withColumn("haveMissing", when($"missing" > 0, 1).otherwise(0))
      .orderBy($"count".desc)
    report
  }
}
