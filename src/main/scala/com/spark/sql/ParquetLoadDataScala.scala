package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * parquet 数据源之使用编程方式加载数据
  */
object ParquetLoadDataScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetLoadDataScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val userDF = sqlContext.read.parquet("/Users/saicao/Desktop/users.parquet")

    userDF.registerTempTable("users")

    val userNameDF = sqlContext.sql("select name from users")

    userNameDF.rdd.map(row => "Name: " + row(0)).collect().foreach(x=>println(x))
  }
}
