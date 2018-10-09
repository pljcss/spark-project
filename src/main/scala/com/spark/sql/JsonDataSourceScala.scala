package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Json 数据源
  */
object JsonDataSourceScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonDataSourceScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val scoreDF = sqlContext.read.json("/Users/saicao/Desktop/studet.json")

    // 注册为临时表, 并查询分数大于 80 的学生
    scoreDF.registerTempTable("student_score")
    val goodScoreDF = sqlContext.sql("select name, score from student_score where score > 80")


    // 构造学生信息数据
    val jsonInfo = Array("{\"name\":\"Leo\", \"age\":\"22\"}",
                     "{\"name\":\"Tom\", \"age\":\"33\"}",
                     "{\"name\":\"Bob\", \"age\":\"28\"}")
    // 生成 JavaRDD
    val studentInfoRDD = sc.parallelize(jsonInfo)

    // 生成 DataFrame
    val studentInfoDF = sqlContext.read.json(studentInfoRDD)

    // 注册临时表
    studentInfoDF.registerTempTable("student_info")

    val ss = sqlContext.sql("select name, age from student_info")

    val sql = "select * from (select name, score from student_score where score > 80) t left join student_info on t.name=student_info.name"

    sqlContext.sql(sql).show()
  }
}
