package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlacklistScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlacklistScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blacklist = Array(("tom", true))

    val blackListRDD = ssc.sparkContext.parallelize(blacklist, 5)

    val adsClickLogStream = ssc.socketTextStream("localhost", 9988)

    val userAdsClickLogStream = adsClickLogStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))

    val validAdsClickLogDStream = userAdsClickLogStream.transform(userAdsClickLogRDD => {
      val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD)
      val filteredRDD = joinedRDD.filter(tuple=>{
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })

      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD
    })

    validAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
