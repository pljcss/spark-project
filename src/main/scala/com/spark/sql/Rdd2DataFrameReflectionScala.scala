package com.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果要用 Scala 开发 Spark 程序
  * 然后, 还要实现基于反射的 RDD 到 DataFrame 的转换, 就必须得用 object extends App 的方式
  * 不能使用 def main 的方式来运行程序, 否则会报 No TypeTag available for Students...的错
  */
object Rdd2DataFrameReflectionScala extends App {

  val conf = new SparkConf().setAppName("Rdd2DataFrameReflectionScala").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // 在 Scala 中使用反射的方式将 RDD 转换为 DataFrame, 需要手动导入一个隐式转换
  import sqlContext.implicits._

  val lines = sc.textFile("/Users/saicao/Desktop/student.txt")

  case class Students(id: Int, name: String, age: Int)

  // 这里其实就是一个普通的, 元素为 case class 的 RDD
  val studentRDD = lines.map(x => x.split(",")).map(arr => Students(arr(0).toInt, arr(1), arr(2).toInt))

  // 直接使用 RDD 的 toDF(), 即可将其转换为DataFrame
  val studentDF = studentRDD.toDF()

  // 注册为临时表
  studentDF.registerTempTable("students")

  // 执行 SQL 语句
  val stuDF = sqlContext.sql("select * from students where age > 30")

  // 将 DataFrame 转换为 RDD, row 类型的
  val stuRDD = stuDF.rdd

  // 将 row 类型转换为 Students 类型
  // 在 Scala 中, row 的顺序与 Java 中的不一样
  stuRDD.map(row => Students(row(0).toString.toString.toInt, row(1).toString,row(2).toString.toInt)).foreach(x=>println(x))

  /**
    * 在 Scala 中, 对 row 的使用, 比 Java 的要丰富
    * 在 Scala 中, 可以使用 getAs() 获取指定列名的列
    */
  stuRDD.map(row => Students(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age"))).foreach( x=> println(x))

  /**
    * 还可以通过 row 的 getValuesMap() 方法获取指定几列的值, 返回的是 map
    */
  stuRDD.map(row => {
    val map = row.getValuesMap[Any](Array("id", "name", "age"))
    Students(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
  }).foreach(x=>println(x))

}
