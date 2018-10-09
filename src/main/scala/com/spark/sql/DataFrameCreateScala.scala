package com.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreateScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameCreateScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建出来的 DataFrame 完全可以理解为一张表
    val df = sqlContext.read.json("/Users/saicao/Desktop/json.txt")

    // 打印所有信息
    df.show()

    // 打印 DataFrame的元数据 (schema)
    df.printSchema()

    // 查询某列的所有数据
    df.select("name").show()

    // 查询某几列的所有数据,并对列进行计算
    df.select(df.col("name"), df.col("age").plus(1)).show()

    // 根据某一列的值进行过滤
    df.filter(df.col("age").gt(20)).show()

    // 根据某一列进行分组,然后聚合
    df.groupBy(df.col("age")).count().show()

  }
}
