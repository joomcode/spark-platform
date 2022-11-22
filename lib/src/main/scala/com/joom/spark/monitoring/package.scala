package com.joom.spark

import java.io.IOException

package object monitoring {

  def using[T <: {def close()}, R](resource: T)(block: T => R): R = {
    try {
      block(resource)
    } finally {
      if (resource != null) {
        try {
          resource.close()
        } catch {
          case e: IOException => println(s"Cannot close resource. $e")
        }
      }
    }
  }
}
