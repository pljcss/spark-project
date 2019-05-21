package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于滑动窗口的热点搜索词实时统计
  */
object WindowHotWordScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowHotWordScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val searchLogsDStream = ssc.socketTextStream("localhost", 9988)

    val searchWordsDStream = searchLogsDStream.map(_.split(" ")(1))

    val searchWordsPairDStream = searchWordsDStream.map(searchWords => (searchWords, 1))

    val searchWordsCountsDStream = searchWordsPairDStream.reduceByKeyAndWindow(
      (v1:Int, v2:Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )

    val finalDStream = searchWordsCountsDStream.transform(searchWordCountsRDD => {
      val countsSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
      val sortedCountsSearchWordsRDD = countsSearchWordsRDD.sortByKey(false)
      val sortedSearchWordCountsRDD = sortedCountsSearchWordsRDD.map(tuple => (tuple._2, tuple._1))
      val top3SearchWords = sortedSearchWordCountsRDD.take(3)
      println(top3SearchWords)
      searchWordCountsRDD
    })

    finalDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
