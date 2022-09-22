package com.joom.spark

import com.joom.spark.implicits.ExplicitRepartitionWrapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatestplus.junit.JUnitRunner

case class Entry(value: String, desired_partition: Int, partition: Int)

@RunWith(classOf[JUnitRunner])
class ExplicitRepartitionSpec extends FlatSpec with Matchers {

  implicit val spark = SparkSession
    .builder
    .master("local[1]")
    .getOrCreate()
  spark.experimental.extraStrategies = Seq(ExplicitRepartitionStrategy)

  "it" should "work" in {
    import spark.implicits._

    val df = Seq(
      ("a", 0),
      ("b", 1),
      ("c", 1),
      ("d", 2)
    ).toDF("value", "desired_partition")

    //val rdf = new ExplicitRepartitionWrapper(df).explicitRepartition(3, $"desired_partition")

    val rdf = df
      .explicitRepartition(3, $"desired_partition")
      .withColumn("partition", org.apache.spark.sql.functions.spark_partition_id())
    rdf.show(100, false)

    val collected = rdf.as[Entry].collect().sortBy(_.value).toSeq
    collected should equal (Seq(
      Entry("a", 0, 0),
      Entry("b", 1, 1),
      Entry("c", 1, 1),
      Entry("d", 2, 2)
    ))
  }

  "repartition via map" should "work" in {
    import spark.implicits._

    val df = Seq(
      ("a"),
      ("b"),
      ("c"),
      ("d")
    ).toDF("value")

    val desiredPartition = typedLit(Map(
      "a" -> 7,
      "b" -> 2,
      "c" -> 0,
      "d" -> 1
    ))

    val rdf = df
      .withColumn("desired_partition", desiredPartition($"value"))
      .explicitRepartition(8, $"desired_partition")
      .withColumn("partition", org.apache.spark.sql.functions.spark_partition_id())
    rdf.show(100, false)

    val collected = rdf.as[Entry].collect().sortBy(_.value).toSeq
    collected should equal (Seq(
      Entry("a", 7, 7),
      Entry("b", 2, 2),
      Entry("c", 0, 0),
      Entry("d", 1, 1)
    ))
  }

}
