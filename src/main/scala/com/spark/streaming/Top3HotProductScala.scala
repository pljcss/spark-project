package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 与Spark SQl 整合使用, top3 热门商品实时统计
  */
object Top3HotProductScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3HotProductScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val productClickLogsDStream = ssc.socketTextStream("localhost", 9988)

    val categoryProductPairsDStream = productClickLogsDStream.map(
      productClickLog => {
        (productClickLog.split(" ")(2) + "_" + productClickLog.split(" ")(1), 1)
      })

    val categoryProductCountsDStream = categoryProductPairsDStream.reduceByKeyAndWindow(
      (v1:Int, v2:Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )

    categoryProductCountsDStream.foreachRDD(categoryProductCountsRDD => {
      val categoryProductCountRowRDD = categoryProductCountsRDD.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)
      })

      val structType = StructType(Array(
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("click_count", IntegerType, true)
      ))

      val hiveContext = new HiveContext(categoryProductCountsRDD.context)

      val categoryProductCountDF = hiveContext.createDataFrame(categoryProductCountRowRDD, structType)

      categoryProductCountDF.registerTempTable("product_click_log")

      val sonSql = "SELECT category, product, click_count, row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank FROM product_click_log"
      val top3ProductDF = hiveContext.sql("SELECT category, product, click_count FROM (" + sonSql + ") tmp WHERE rank <= 3")

      top3ProductDF.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
