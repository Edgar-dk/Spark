package com.core.rdd.operator.action

/**
 * @author Edgar
 * @create 2022-11-29 15:11
 * @faction:
 */
object Test {
  def main(args: Array[String]): Unit = {
    val d = new D()
    println(d.a)
    println(d.b.getClass.getSimpleName)
  }
}

class D {
  var a: Int = _
  var b: String = _
}
