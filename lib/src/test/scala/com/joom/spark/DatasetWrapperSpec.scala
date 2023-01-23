package com.joom.spark

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner
import com.joom.spark.implicits._

@RunWith(classOf[JUnitRunner])
class DatasetWrapperSpec extends FlatSpec with Matchers {
  implicit def toOption[T](v: T): Some[T] = Some(v)

  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()
  import spark.implicits._

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
}

case class Device(product_id: Option[String], device_id: Option[String], map: Map[String, Int], arr: Array[String])

case class Product(product_id: Option[String], product_info: ProductInfo)
case class ProductInfo(a: Option[String], b: Int)
