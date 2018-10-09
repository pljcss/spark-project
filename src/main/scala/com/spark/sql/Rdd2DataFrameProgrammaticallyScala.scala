package com.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 以编程方式动态指定元数据, 将 RDD 转换为 DataFrame
  */
object Rdd2DataFrameProgrammaticallyScala extends App {

  val conf = new SparkConf().setAppName("Rdd2DataFrameProgrammaticallyScala").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // 第一步, 构造元素为 Row 的RDD
  val studentRDD = sc.textFile("/Users/saicao/Desktop/student.txt")
    .map(line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt))

  // 第二步, 编程方式动态构造元数据
  val structType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  // 第三步, 进行 RDD 到 DataFrame 转换
  val studentDF = sqlContext.createDataFrame(studentRDD, structType)

  // 注册表
  studentDF.registerTempTable("students")

  val teenagerDF = sqlContext.sql("select * from students where age > 30")

  teenagerDF.rdd.foreach(row => println(row))

}
