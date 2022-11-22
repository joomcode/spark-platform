package com.joom.spark

import com.joom.spark.types._
import org.apache.spark.sql.types._
import org.scalatest._

class TypesSpec extends FlatSpec with Matchers {
  "Struct type" should "be equal to itself" in {
    val jsonStruct =
      """
        |{
        |  "type": "struct",
        |  "fields": [
        |    {
        |      "name": "order_id",
        |      "type": "string",
        |      "nullable": true,
        |      "metadata": {}
        |    }
        |  ]
        |}
      """.stripMargin

    val dataType = DataType.fromJson(jsonStruct)
    compareTypes(dataType.asInstanceOf[StructType], dataType.asInstanceOf[StructType]) should have length 0
  }

  "Long type" should "be equal to itself" in {
    compareTypes(DataTypes.LongType, DataTypes.LongType) should have length 0
  }

  "Long type" should "be different from Double" in {
    compareTypes(DataTypes.LongType, DataTypes.DoubleType) should have length 1
  }

  "Schema metadata builder" should "work" in {
    val schema = StructType(Array(
      StructField("x", ArrayType(StringType)),
      StructField("y", IntegerType),
      StructField("z", StructType(Array(
        StructField("a", DateType),
        StructField("b", ArrayType(BooleanType)),
        StructField("c", MapType(ArrayType(StringType), IntegerType))
      )))
    ))

    val f: Metadata => Metadata = metadata => {
      new MetadataBuilder().withMetadata(metadata).putStringArray("source", Array("test")).build()
    }

    val schemaWithMetadata = types.mapFieldMetadata(schema, f)

    println(schemaWithMetadata.prettyJson)
  }
}
