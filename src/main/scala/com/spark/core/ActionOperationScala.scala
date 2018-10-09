package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * action算子
  */
object ActionOperationScala {
  def main(args: Array[String]): Unit = {
    collect()
  }

  /**
    * reduce 算子
    * count 算子,统计 RDD 中有多少个元素
    */
  def reduce(): Unit = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)

    val rdd = sc.parallelize(numbers)

    println(rdd.count())

    val res = rdd.reduce(_+_)

    println(res)
  }

  /**
    * collect 算子
    * take 算子
    */
  def collect(): Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)

    val rdd = sc.parallelize(numbers)

    val res = rdd.map(x => x*2)

    val list: Array[Int] = res.collect()

    for (x <- list) println(x)
  }


}
