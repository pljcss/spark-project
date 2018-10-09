package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object GenericLoadSaveScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GenericLoadSaveScala").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // load 操作
    val df = sqlContext.read.load("/Users/saicao/Desktop/users.parquet")

    df.show()

    // save 操作
    df.select("name", "favorite_color").write.save("/Users/saicao/Desktop/tt.parquet")
  }
}
