package com.joom.spark

import org.apache.spark.sql.types._

/** Various utilities to work with Spark SQL types. */
object types {
  /** Potential differences between two data types */
  sealed trait DataTypeDiff
  case class LeftOnly(path: String) extends DataTypeDiff {
    override def toString: String = s"$path missing on right"
  }
  case class RightOnly(path: String) extends DataTypeDiff {
    override def toString: String = s"$path missing on left"
  }
  case class TypeMismatch(path: String, left: DataType, right: DataType) extends DataTypeDiff {
    override def toString: String = s"$path has type ${left.json} on left, ${right.json} on right"
  }
  case class NullableMismatch(path: String, leftNullability: Boolean, rightNullability: Boolean) extends DataTypeDiff {
    override def toString: String = s"$path has nullability $leftNullability on left, $rightNullability on right"
  }
  case class MetadataMismatch(path: String, leftMetadata: Metadata, rightMetadata: Metadata) extends DataTypeDiff {
    override def toString: String = s"$path has metadata $leftMetadata on left, $rightMetadata on right"
  }

  /**
    * Recursively compare two types and return a list of differences as human-readable strings.
    * @param leftType the 1st type to compare
    * @param rightType the 2nd type to compare
    * @return detected differences between the two types
    */
  def compare(leftType: DataType, rightType: DataType, checkNullable: Boolean = false, checkMetadata: Boolean = false): Seq[DataTypeDiff] = {
    def compareDataTypes(leftType: DataType, rightType: DataType, path: String): Seq[DataTypeDiff] = {
      (leftType, rightType) match {
        case (lt: StructType, rt: StructType) =>
          compareStructTypes(lt, rt, path)

        case (lt: MapType, rt: MapType) =>
          compareDataTypes(lt.keyType, rt.keyType, path + ".key") ++ compareDataTypes(lt.valueType, rt.valueType, path + ".value")

        case (lt: ArrayType, rt: ArrayType) =>
          compareDataTypes(lt.elementType, rt.elementType, path + ".<elementType>")

        case (l, r) if l != r =>
          Seq(TypeMismatch(path, l, r))

        case _ => Seq()
      }
    }

    def compareStructTypes(leftSchema: StructType, rightSchema: StructType, path: String = "root"): Seq[DataTypeDiff] = {
      val ids = (leftSchema.map(_.name) ++ rightSchema.map(_.name)).distinct

      val leftFields = leftSchema.fields.map(it => it.name -> it).toMap
      val rightFields = rightSchema.fields.map(it => it.name -> it).toMap
      ids.flatMap(id => {
        (leftFields.get(id), rightFields.get(id)) match {
          case (None, _) => Seq(RightOnly(s"$path.$id"))

          case (_, None) => Seq(LeftOnly(s"$path.$id"))

          case (Some(a), Some(b)) =>
            val nm = if (checkNullable && a.nullable != b.nullable) {
              Seq(NullableMismatch(path, a.nullable, b.nullable))
            } else Seq.empty
            val mm = if (checkMetadata && a.metadata != b.metadata) {
              Seq(MetadataMismatch(path, a.metadata, b.metadata))
            } else Seq.empty
            nm ++ mm ++  compareDataTypes(a.dataType, b.dataType, path + "." + id)
        }
      })
    }

    compareDataTypes(leftType, rightType, "root")
  }

  /** Recursively compare two types and return a list of differences as human-readable strings. */
  def compareTypes(leftType: DataType, rightType: DataType, path: String = "root"): Seq[String] = {
    compare(leftType, rightType).map(_.toString)
  }

  def mapFieldMetadata(schema: StructType, f: Metadata => Metadata): StructType = {
    def recurse(dataType: DataType): DataType = {
      dataType match {
        case StructType(fields) =>
          DataTypes.createStructType(fields.map { field =>
            StructField(
              field.name,
              recurse(field.dataType),
              field.nullable,
              f(field.metadata)
            )
          })

        case t => t
      }
    }
    recurse(schema).asInstanceOf[StructType]
  }

  /**
    * @param schema    record schema
    * @param fieldPath comma-separated full path to the target field
    * @param fieldType expected target field type
    * @return true if the field exists in the schema and has the expected type
    */
  def exists(schema: StructType, fieldPath: String, fieldType: DataType): Boolean = {
    fieldAt(schema, fieldPath).exists(field => field.dataType == fieldType)
  }

  /**
    * @param schema    record schema
    * @param fieldPath comma-separated full path to the target field
    * @return field at the given path
    */
  def fieldAt(schema: StructType, fieldPath: String): Option[StructField] = {
    def nestedExists(level: StructType, path: Seq[String]): Option[StructField] = {
      if (path.isEmpty) {
        throw new IllegalArgumentException("Field path is required")
      }

      level.fields.find(fld => fld.name == path.head).flatMap(field => {
        val tail = path.tail

        if (tail.isEmpty) {
          Some(field)
        } else {
          field.dataType match {
            case st: StructType =>
              nestedExists(st, tail)

            case at: ArrayType =>
              at.elementType match {
                case ast: StructType =>
                  nestedExists(ast, tail)

                case _ => throw new IllegalArgumentException(s"Path not found: ${path.mkString("/")}")
              }

            case _ => throw new IllegalArgumentException(s"Path not found: ${path.mkString("/")}")
          }
        }
      })
    }

    nestedExists(schema, fieldPath.split('.'))
  }
}
