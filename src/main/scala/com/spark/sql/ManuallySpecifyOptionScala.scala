package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ManuallySpecifyOptionScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GenericLoadSaveScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 读取 json 格式的文件, 写成 parquet 格式的文件
    val df = sqlContext.read.format("json").load("/Users/saicao/Desktop/json.txt")

    df.show()

    df.select("name").write.format("parquet").save("/Users/saicao/Desktop/jsonParquet.parquet")
  }
}
