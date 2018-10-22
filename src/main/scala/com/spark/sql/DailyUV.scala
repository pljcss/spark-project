package com.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailyUV").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 要使用Spark SQL的内置函数, 就必须导入SQLContext 下的隐士转换
      */
    import sqlContext.implicits._


    // 构造用户访问日志数据, 并创建DataFrame
    // 用户访问日志, 第一列是日期, 第二列是用户ID
    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1123",
      "2015-10-02,1123")
    val userAccessLogRDD = sc.parallelize(userAccessLog, 5)

    // 将用户访问日志RDD转换为DataFrame
    // 首先将普通RDD, 转换为元素为Row的 RDD
    val userAccessLogRowRDD = userAccessLogRDD.map(log => Row(log.split(",")(0), log.split(",")(1).toInt))

    // 构造 DataFrame的元数据
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userid", IntegerType, true)))

    // 使用 SQLContext创建 DataFrame
    val userAccessLogRowDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType)

    // 使用 内置函数countDistinct
    // 先按 date分组, 然后按 date进行聚合, 对每一组去重统计总数
    /**
      * 首先,调用DF的 groupBy()方法, 对某一列进行分组
      * 然后调用 agg()方法, 第一个参数是之前传入 groupBy()的字段
      * 第二个参数, 传入 countDistinct、sum、first等, spark提供的内置函数
      * 使用单引号作为前缀
      */
    import org.apache.spark.sql.functions.countDistinct
    userAccessLogRowDF.groupBy("date").agg('date, countDistinct('userid)).map(row => Row(row(1), row(2)))
      .collect().foreach(println)



  }
}
