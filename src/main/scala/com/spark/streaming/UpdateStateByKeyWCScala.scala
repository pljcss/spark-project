package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于 updateStateByKey 算子实现缓存机制的 wordcount 程序
  */
object UpdateStateByKeyWCScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("UpdateStateByKeyWCScala").setMaster("local[2]")
    // scala 中是 StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置 checkpoint
    ssc.checkpoint("hdfs://greentown//test/wc_update")

    val lines = ssc.socketTextStream("localhost", 9988)

    val pairs = lines.flatMap(x => x.split(" ")).map(word => (word, 1))

    val wordcount = pairs.updateStateByKey((values: Seq[Int], state:Option[Int])=>{
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }

      Option(newValue)
    })

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
