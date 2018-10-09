package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariableScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariableScala").setMaster("local")
    val sc = new SparkContext(conf)

    // 定义变量
    val factor = 2
    // 广播变量
    val broadcastFactor = sc.broadcast(factor)

    // 从集合创建RDD
    val numbers = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbers)
    // 获取广播变量
    val multiRDD = numbersRDD.map(x => x * broadcastFactor.value)

    multiRDD.foreach(x => println(x))

  }
}
