package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorVariableScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorVariableScala").setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0)

    // 从集合创建RDD
    val numbers = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbers)

    // 调用add或 +=
    numbersRDD.foreach(x => sum.add(x))
//    numbersRDD.foreach(x => sum += x)

    println(sum.value)
  }
}
