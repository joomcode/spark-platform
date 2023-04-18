package com.joom.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class StatsListenerSpec  extends FlatSpec with Matchers with BeforeAndAfter {
  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.extraListeners", "com.joom.spark.monitoring.StatsReportingSparkListener")
    .getOrCreate()

  "everything" should "work" in {
    Random.setSeed(616001)

    import spark.implicits._

    Seq(1, 2, 3).toDF().show()
  }

  after {
    spark.stop()
  }
}
