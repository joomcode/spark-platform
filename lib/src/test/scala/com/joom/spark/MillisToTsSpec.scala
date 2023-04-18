package com.joom.spark

import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date}
import org.apache.spark.sql.types.LongType
import org.scalatest._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

case class Event(event_ts: Option[Long])

@RunWith(classOf[JUnitRunner])
class MillisToTsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()

  override def afterAll() = {
    spark.stop()
  }

  "millis_to_ts" should "work" in {
    import spark.implicits._
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val df = Seq(1575891193535L)
      .toDF("millis")
      .withColumn("ts", millis_to_ts($"millis"))
      .withColumn("tss", date_format($"ts", "yyyy-MM-dd HH:mm:ss"))

    val row = df.collect()(0)
    row.getString(2) shouldBe "2019-12-09 11:33:13"
  }

  "millis_to_ts" should "accept complex expressions" in {
    import spark.implicits._
    val df = spark.createDataFrame(List(Event(Some(1575891193535L)))).as("input").cache()

    val df2 = df
      .filter(to_date(millis_to_ts(($"event_ts" + 3 * 3600 * 1000).cast(LongType))) === "2019-07-28")

    df2.count() shouldBe 0
  }
}
