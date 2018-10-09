package com.scala.test

object Test {
  def main(args: Array[String]): Unit = {
    // 创建对象
    val p = new Person("Leo", 22)
    println(p)
  }
}

/**
  * 禁用主构造函数, 使用辅助构造函数来创建对象
  */
class Person (var name:String, var age:Int) {
  // 类成员
  private var sex:String = null



  // 辅助构造函数
  def this(name:String, age:Int, sex:String) {
    this(name, age)
    this.sex = sex
  }


}

/**
  * 禁用主构造器
  * 类名后紧跟着private关键字可以将主构造器设为私有, 不允许外部使用
  */
class Animal private(var name:String="Dog", var age:Int=18) {

  // println将作为主构建器中的一部分，在创建对象时被执行
  println("constructing Person ........")

  // 重写 toString
  override def toString: String = name + ", " + age

}

