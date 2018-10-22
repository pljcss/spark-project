package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWordCountScala").setMaster("local[2]")
    // scala 中是 StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("hdfs://.../word_dir")

    val wordcounts = lines.flatMap(x=>x.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    Thread.sleep(5000)
    wordcounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
