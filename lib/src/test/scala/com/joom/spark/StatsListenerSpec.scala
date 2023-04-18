package com.joom.spark

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class StatsListenerSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.extraListeners", "com.joom.spark.monitoring.StatsReportingSparkListener")
    .getOrCreate()

  override def afterAll() = {
    spark.stop()
  }

  "everything" should "work" in {
    Random.setSeed(616001)

    import spark.implicits._

    Seq(1, 2, 3).toDF().show()
  }
}
