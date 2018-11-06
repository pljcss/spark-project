package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * UDAF
  */
object UDAFScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAFScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("Leo", "Bob", "Jack", "Jack", "Jack")
    val namesRDD = sc.parallelize(names, 5)

    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val nameDF = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册一张临时表
    nameDF.registerTempTable("names_table")

    /**
      * 定义和注册 自定义函数
      * 定义函数, 自己写匿名函数
      * sqlContext.udf.register()
      */
    sqlContext.udf.register("strCount", new StringCountUDAFScala)

    // 使用自定义函数
    sqlContext.sql("select name, strCount(name) from names_table group by name").collect().foreach(println)

  }
}
