package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDDFromLocalFileScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateRDDFromLocalFileScala").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/Users/saicao/Desktop/file1.txt")

    val lengthRdd = rdd.map(_.length)

    val nums = lengthRdd.reduce(_+_)

    println(nums)
  }
}
