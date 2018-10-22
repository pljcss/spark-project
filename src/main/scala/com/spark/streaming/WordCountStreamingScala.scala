package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时 wordcount 程序
  */
object WordCountStreamingScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountStreamingScala").setMaster("local[2]")

    // scala 中是 StreamingContext
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val wordcounts = lines.flatMap(x=>x.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    Thread.sleep(5000)
    wordcounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
