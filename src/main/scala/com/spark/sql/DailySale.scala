package com.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
object DailySale {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailySale").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 要使用Spark SQL的内置函数, 就必须导入SQLContext 下的隐士转换
      */
    import sqlContext.implicits._

    // 模拟数据, 时间,销售额,用户id
    val userSaleLog = Array(
      "2015-10-01,22,1122",
      "2015-10-01,11,1122",
      "2015-10-01,11,",
      "2015-10-02,5,1133",
      "2015-10-02,6,1133",
      "2015-10-02,7,")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)

    // 有效销售日志过滤
    val filteredUserSaleLogRDD = userSaleLogRDD.filter(log => if(log.split(",").length==3) true else false)

    val userSaleLogRowRDD = filteredUserSaleLogRDD.map(log =>
      Row(log.split(",")(0), log.split(",")(1).toInt))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_count", IntegerType, true)))

    val userAccessLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)

    // 需要导入, import org.apache.spark.sql.functions._
    userAccessLogDF.groupBy("date").agg('date, sum('sale_count)).map(row => Row(row(1), row(2)))
      .collect().foreach(println)




  }
}
