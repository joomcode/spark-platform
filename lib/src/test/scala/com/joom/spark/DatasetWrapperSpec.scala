package com.joom.spark

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner
import com.joom.spark.implicits._
import org.apache.spark.sql.functions.{col, lit, spark_partition_id, when}

@RunWith(classOf[JUnitRunner])
class DatasetWrapperSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit def toOption[T](v: T): Some[T] = Some(v)

  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()
  import spark.implicits._

  override def afterAll() = {
    spark.stop()
  }

  "joinAvoidSkewDueToNulls" should "give a result identical to the standard join" in {
    val devices = Seq(
      Device("pr1", "d1", Map("d1" -> 1), Array("d11", "d12")),
      Device("pr2", "d2", Map("d2" -> 2), Array("d21", "d22")),
      Device("pr2", None, Map("d3" -> 3), Array("d31", "d32")),
      Device("pr5", "d5", Map("d5" -> 5), Array("d51", "d52")),
      Device(None, "d4", Map("d4" -> 4), Array("d41", "d42")),
      Device(None, "d5", Map("d5" -> 5), Array("d51", "d52")),
    ).toDF().as("d")

    val products = Seq(
      Product("pr1", ProductInfo("p1", 1)),
      Product("pr2", ProductInfo(None, 2)),
      Product("pr3", ProductInfo("p3", 3)),
      Product(None, ProductInfo("p4", 4)),
    ).toDF().as("p")

    Seq("inner", "left", "right", "full", "left_anti").foreach(joinType =>
      devices.joinAvoidSkewDueToNulls(products, "product_id", "product_id", joinType).collect()
        should contain theSameElementsAs
        devices.join(products, $"d.product_id" === $"p.product_id", joinType).collect()
    )
  }

  it should "preserve numeric keys and unmatched nulls with ANSI mode enabled" in {
    val previousAnsi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try {
      val left = Seq((Some(1), "left-one"), (Some(2), "left-two"), (None, "left-null"))
        .toDF("key", "left_value").as("l")
      val right = Seq((Some(1), "right-one"), (Some(3), "right-three"), (None, "right-null"))
        .toDF("key", "right_value").as("r")

      Seq("inner", "left", "right", "full", "left_anti").foreach { joinType =>
        val actual = left.joinAvoidSkewDueToNulls(right, "key", "key", joinType).collect()
        val expected = left.join(right, $"l.key" === $"r.key", joinType).collect()
        actual should contain theSameElementsAs expected
      }
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previousAnsi)
    }
  }

  it should "preserve numeric coercion and signed-zero equality with ANSI mode enabled" in {
    val previousAnsi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try {
      val intKeys = Seq((Some(1), "left-one"), (Some(2), "left-two"), (None, "left-null"))
        .toDF("key", "left_value")
      val doubleKeys = Seq((Some(1.0), "right-one"), (Some(3.0), "right-three"), (None, "right-null"))
        .toDF("key", "right_value")
      assertMatchesStandardJoin(intKeys, doubleKeys)

      val negativeZero = Seq((Some(-0.0), "left-zero"), (None, "left-null"))
        .toDF("key", "left_value")
      val positiveZero = Seq((Some(0.0), "right-zero"), (None, "right-null"))
        .toDF("key", "right_value")
      assertMatchesStandardJoin(negativeZero, positiveZero)
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previousAnsi)
    }
  }

  it should "keep synthetic null keys separate from real string keys" in {
    val left = Seq((Option.empty[String], "left-null")).toDF("key", "left_value")
    // Reproduce a real key that collides with the former string sentinel.
    val formerNullKey = left.selectExpr("concat('left', cast(hash(key, left_value) as string))")
      .as[String].head()
    val right = Seq((Some(formerNullKey), "right-real"), (None, "right-null"))
      .toDF("key", "right_value")

    assertMatchesStandardJoin(left, right)
  }

  it should "use Spark scalar coercion for decimal, string and date keys" in {
    val previousAnsi = spark.conf.get("spark.sql.ansi.enabled")
    try {
      Seq("false", "true").foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        val decimals = spark.sql("SELECT cast(1.20 AS decimal(3, 2)) AS key, 1 AS left_value")
        val otherDecimals = spark.sql("SELECT cast(1.2 AS decimal(2, 1)) AS key, 2 AS right_value")
        assertMatchesStandardJoin(decimals, otherDecimals)

        val strings = Seq((Some("01"), "left-one"), (None, "left-null"))
          .toDF("key", "left_value")
        val numbers = Seq((Some(1), "right-one"), (None, "right-null"))
          .toDF("key", "right_value")
        assertMatchesStandardJoin(strings, numbers)
        if (ansi == "false") {
          val malformed = Seq((Some("invalid"), "left-invalid"), (None, "left-null"))
            .toDF("key", "left_value")
          assertMatchesStandardJoin(malformed, numbers)
        }

        val dates = spark.sql("SELECT DATE '2020-01-01' AS key, 1 AS left_value")
        val timestamps = spark.sql("SELECT TIMESTAMP '2020-01-01 00:00:00' AS key, 2 AS right_value")
        assertMatchesStandardJoin(dates, timestamps)
      }
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previousAnsi)
    }
  }

  it should "preserve input columns named like the temporary join key" in {
    val left = Seq((Some("one"), "left-one"), (None, "left-null"))
      .toDF("key", "_joinColName")
    val right = Seq((Some("one"), "right-one"), (None, "right-null"))
      .toDF("key", "_joinColName")

    assertMatchesStandardJoin(left, right)
  }

  it should "handle literal dots and backticks in input field names" in {
    val left = Seq((Some("one"), "left-one", 1), (None, "left-null", 2))
      .toDF("key", "left.value", "left`value")
    val right = Seq((Some("one"), "right-one", 1), (None, "right-null", 2))
      .toDF("key", "right.value", "right`value")

    assertMatchesStandardJoin(left, right)
  }

  it should "support joining a dataset with itself" in {
    val input = Seq((Some(1), "one"), (Some(1), "another-one"), (None, "null"))
      .toDF("key", "value")
    val actual = input.joinAvoidSkewDueToNulls(input, "key", "key", "inner")
    val l = input.as("l")
    val r = input.as("r")
    val expected = l.join(r, l("key") === r("key"), "inner")
    actual.collect() should contain theSameElementsAs expected.collect()
  }

  it should "spread unmatched null rows across shuffled join partitions" in {
    val settings = Map("spark.sql.adaptive.enabled" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1", "spark.sql.shuffle.partitions" -> "4")
    val previous = settings.keys.map(key => key -> spark.conf.get(key)).toMap
    settings.foreach { case (key, value) => spark.conf.set(key, value) }
    try {
      val left = spark.range(0, 400, 1, 4).select(
        when(col("id") % 2 === 0, lit(null).cast("long")).otherwise(col("id")).as("key"),
        col("id").as("left_value"))
      val right = spark.range(0, 400, 1, 4).select(col("id").as("key"), col("id").as("right_value"))
      val result = left.joinAvoidSkewDueToNulls(right, "key", "key", "left")
        .withColumn("result_partition", spark_partition_id())
      val rows = result.collect()
      val nullRows = rows.filter(_.isNullAt(0))
      nullRows.length shouldBe 200
      withClue(result.queryExecution.executedPlan.toString()) {
        nullRows.map(row => row.getInt(row.length - 1)).distinct.length should be > 1
      }
    } finally {
      previous.foreach { case (key, value) => spark.conf.set(key, value) }
    }
  }

  if (spark.version.startsWith("4.")) {
    it should "preserve the collation of string keys" in {
      val left = spark.sql("SELECT collate('A', 'UTF8_LCASE') AS key, 1 AS left_value").as("l")
      val right = spark.sql("SELECT collate('a', 'UTF8_LCASE') AS key, 2 AS right_value").as("r")
      val actual = left.joinAvoidSkewDueToNulls(right, "key", "key", "inner").collect()
      val expected = left.join(right, $"l.key" === $"r.key", "inner").collect()
      expected.length shouldBe 1
      actual should contain theSameElementsAs expected
    }
  }

  private def assertMatchesStandardJoin(left: DataFrame, right: DataFrame): Unit = {
    val l = left.as("l")
    val r = right.as("r")
    Seq("inner", "left", "right", "full", "left_anti").foreach { joinType =>
      withClue(s"join type: $joinType: ") {
        // Older Spark ANSI analyzers reject some mixed-type comparisons.
        // The helper must follow that version's SQL rules, including rejection.
        val expected = try {
          val result = l.join(r, l("key") === r("key"), joinType)
          result.queryExecution.analyzed
          Some(result)
        } catch {
          case _: AnalysisException => None
        }
        expected match {
          case Some(result) =>
            val actual = l.joinAvoidSkewDueToNulls(r, "key", "key", joinType)
            actual.schema.fieldNames.toSeq shouldBe result.schema.fieldNames.toSeq
            actual.collect() should contain theSameElementsAs result.collect()
          case None =>
            intercept[AnalysisException] {
              l.joinAvoidSkewDueToNulls(r, "key", "key", joinType).queryExecution.analyzed
            }
        }
      }
    }
  }
}

case class Device(product_id: Option[String], device_id: Option[String], map: Map[String, Int], arr: Array[String])

case class Product(product_id: Option[String], product_info: ProductInfo)
case class ProductInfo(a: Option[String], b: Int)
