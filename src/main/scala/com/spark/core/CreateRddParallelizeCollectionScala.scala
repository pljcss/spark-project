package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CreateRddParallelizeCollectionScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateRddParallelizeCollectionScala").setMaster("local")

    val sc = new SparkContext(conf)

    val list = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val rdd = sc.parallelize(list, 5)

    val nums = rdd.reduce(_+_)

    println(nums)
  }
}
