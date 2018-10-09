package com.scala.test

object Test1 {
  def main(args: Array[String]): Unit = {

    val x = collection.immutable.LinearSeq("a", "b", "c")
    val head = x.head
    println(s"head is: $head")

    val y = x.tail
    println(s"tail of y is: $y")

    println(x)

  }
}
