package com.joom.spark

import com.joom.spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeduplicateWithinPartitionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .master("local[1]")
    .appName("spark-platform-deduplication-test")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  spark.experimental.extraStrategies = Seq(DeduplicateWithinPartitionsStrategy, ExplicitRepartitionStrategy)

  override def afterAll(): Unit = {
    try spark.stop() finally super.afterAll()
  }

  "deduplicateWithinPartitions" should "keep the first row in the requested sort order" in {
    import spark.implicits._
    val input = Seq(
      ("b", 2, "b-later"),
      ("a", 3, "a-later"),
      ("b", 1, "b-first"),
      ("a", 1, "a-first"),
      ("c", 1, "c-first"),
      ("a", 2, "a-middle")
    ).toDF("key", "sequence", "payload").repartition(1)

    val result = input.deduplicateWithinPartitions(Seq("key", "sequence"), Seq("key"))
      .collect().map(row => (row.getString(0), row.getInt(1), row.getString(2)))
      .toVector.sortBy(_._1)

    result shouldBe Vector(("a", 1, "a-first"), ("b", 1, "b-first"), ("c", 1, "c-first"))
  }

  it should "preserve separate occurrences of a key in different partitions" in {
    import spark.implicits._
    val input = Seq(
      ("a", 0, 2),
      ("a", 1, 4),
      ("a", 0, 1),
      ("a", 1, 3)
    ).toDF("key", "desired_partition", "sequence")
      .explicitRepartition(2, $"desired_partition")

    // A global dropDuplicates would lose one partition's row and violate this API.
    val result = input.deduplicateWithinPartitions(Seq("key", "sequence"), Seq("key"))
      .withColumn("actual_partition", spark_partition_id())
      .collect().map(row => (row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3)))
      .toVector.sortBy(_._2)

    result shouldBe Vector(("a", 0, 1, 0), ("a", 1, 3, 1))
  }

  it should "compare signed zero keys using SQL equality" in {
    import spark.implicits._
    val input = Seq(
      (-0.0, 2, "zero-later"),
      (0.0, 1, "zero-first"),
      (1.0, 1, "one")
    ).toDF("key", "sequence", "payload").repartition(1)

    val result = input.deduplicateWithinPartitions(Seq("key", "sequence"), Seq("key"))
      .collect().map(row => row.getString(2)).toVector.sorted

    result shouldBe Vector("one", "zero-first")
  }

  it should "compare NaN keys using SQL equality" in {
    import spark.implicits._
    val input = Seq(
      (java.lang.Double.longBitsToDouble(0x7ff8000000000001L), 2, "nan-later"),
      (java.lang.Double.longBitsToDouble(0x7ff8000000000002L), 1, "nan-first"),
      (1.0, 1, "one")
    ).toDF("key", "sequence", "payload").repartition(1)

    // The sequence determines which NaN row survives; comparing NaN payloads
    // directly would itself use a different equality rule outside Spark SQL.
    val result = input.deduplicateWithinPartitions(Seq("key", "sequence"), Seq("key"))
      .collect().map(row => row.getString(2)).toVector.sorted

    result shouldBe Vector("nan-first", "one")
  }

  if (spark.version.startsWith("4.")) {
    it should "respect string collation when comparing keys" in {
      import spark.implicits._
      val input = Seq(("a", 2, "later"), ("A", 1, "first"))
        .toDF("key", "sequence", "payload")
        .selectExpr("collate(key, 'UTF8_LCASE') AS key", "sequence", "payload")
        .repartition(1)

      val result = input.deduplicateWithinPartitions(Seq("key", "sequence"), Seq("key"))
        .collect().map(row => (row.getString(0), row.getInt(1), row.getString(2))).toVector

      result shouldBe Vector(("A", 1, "first"))
    }
  }
}
