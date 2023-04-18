package com.joom.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll }
import org.scalatestplus.junit.JUnitRunner

import scala.util.Random

case class Result(source: Long, duplicate: Long, missing: Long)

@RunWith(classOf[JUnitRunner])
class CheckEventSequenceSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.default.parallelism", "20")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()

  override def afterAll() = {
    spark.stop()
  }

  "everything" should "work" in {
    Random.setSeed(616001)

    // Generate all possible combination of event sequences where sequence values are in [0,4), and where each
    // sequence value occurs either 0, 1, or 2 times.
    val simpleTestData = for {
      c1 <- 0 to 2; c2 <- 0 to 2; c3 <- 0 to 2; c4 <- 0 to 2
    } yield (Seq.fill(c1)(0) ++ Seq.fill(c2)(1) ++ Seq.fill(c3)(2) ++ Seq.fill(c4)(3))

    // And generate random pairs of buckets with random gaps between them
    val pairsTestData = (0 to 100).map { _ =>
      val s1 = simpleTestData(Random.nextInt(simpleTestData.size))
      val s2 = simpleTestData(Random.nextInt(simpleTestData.size))
      val gap = Random.nextInt(3)
      s1 ++ s2.map(_ + gap*4)
    }

    val testData = (simpleTestData ++ pairsTestData).filter(_.size > 0)

    import spark.implicits._
    val df = testData.zipWithIndex
      .toDF("sequence", "source")
      .withColumn("counter", explode($"sequence"))
      .drop("sequence")
      .withColumn("type", lit("t")).withColumn("ts", lit("ts"))

    val r = CheckEventSequence(df, "source", "counter", "type", "ts", bucketSizeBits = 2)
      .select("source", "duplicate", "missing")
      .as[Result]
      .collect()
      .sortBy(_.source)

    r.size shouldBe(testData.size)

    r.zip(testData).foreach { case (r, td) =>
      val unique = td.toSet.size
      val duplicates = td.size - unique
      val missing = (td.max - td.min + 1) - unique
      r.duplicate should be(duplicates)
      r.missing should be(missing)
    }
  }

}
