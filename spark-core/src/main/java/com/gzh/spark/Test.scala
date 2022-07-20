package com.gzh.spark

object Test {

  def main(args: Array[String]): Unit = {
    println("hello")

    def add(a: Int): Int=>Int = a + _

    println(add(8)(10))

    val list = List(1,2,3,4,5,6)
    list.map( x => x * x )

  }
}
