package com.joom.spark

import com.joom.spark.monitoring.StatsReportingSparkListener
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

  "sigma" should "work" in {
    import StatsReportingSparkListener.sigma

    sigma(Seq.empty[Double]) should be(0.0)
    sigma(Seq(7)) should be(0.0)
    // Mean is 2, Variance is (1 + 0 + 1)/3 = 0.666, SD = 0.81
    StatsReportingSparkListener.sigma(Seq(1, 2, 3)) should be(0.81 +- 0.1)
  }
}
