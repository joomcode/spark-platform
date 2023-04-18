package com.joom.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatestplus.junit.JUnitRunner
import com.joom.spark.implicits._

@RunWith(classOf[JUnitRunner])
class DropNestedColumnsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val spark = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  override def afterAll() = {
    spark.stop()
  }

  "it" should "remove exising nested" in {
    import spark.implicits._

    val schema = Seq(
      Value(Nested(a = "a", b = "b"))
    ).toDF()
      .dropNestedColumn("nested.a")
      .schema

    val nestedSchema = schema.fields(schema.fieldIndex("nested")).dataType.asInstanceOf[StructType]
    nestedSchema.fields.length shouldBe 1
    nestedSchema.fieldIndex("b") shouldBe 0
  }

  "it" should "remove not nested" in {
    import spark.implicits._

    val schema = Seq(
      Value(Nested(a = "a", b = "b"))
    ).toDF()
      .dropNestedColumn("nested")
      .schema

    schema.fields.length shouldBe 0
  }

  "it" should "not fail on not existing nested" in {
    import spark.implicits._

    val schema = Seq(
      Value(Nested(a = "a", b = "b"))
    ).toDF()
      .dropNestedColumn("nested.x")
      .schema

    val nestedSchema = schema.fields(schema.fieldIndex("nested")).dataType.asInstanceOf[StructType]
    nestedSchema.fields.length shouldBe 2
  }
}

case class Nested(a: String, b: String)
case class Value(nested: Nested)
