package com.scala.test

/**
  * 伴生类和伴生对象
  */

// 伴生对象
object Student {

  private var score:Int = 60

  // 直接访问伴生对象的私有成员
  def increaseScore() = {
    score += 1 // 加 1
    score // 返回 score
  }

  // 定义 apply方法, 实例化伴生类
  def apply(name:String, age:String) = new Student(name, age)

  def main(args: Array[String]): Unit = {
    // 单例对象
    println("单例对象, " + Student.increaseScore())

    // 实例化伴生类
    val stu1 = new Student("Leo", "22")
    stu1.printInfo()

    // 通过伴生对象的apply方法访问伴生类成员
    val stu2 = Student("Leo_apply", "22_apply") // 实际上是通过apply方法进行实例化对象, 避免了手动 new 对象
    println(stu2.name)
    println(stu2.age)
  }

}

// 伴生类
class Student(var name:String, var age:String) {
  private var address:String = "china"

  // 直接访问伴生对象的私有成员
  def printInfo(): Unit = {
    println("伴生类中访问伴生对象, " + Student.score)
  }
}