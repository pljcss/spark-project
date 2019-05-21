package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object LineCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCountScala").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/saicao/Desktop/file_test/word.txt")

    println(lines.count())

    val pairRDD = lines.map(lines => (lines, 1))

    println(pairRDD.groupByKey().count())

    val linesCounts = pairRDD.reduceByKey(_+_)

    linesCounts.foreach(x => println(x._1 + " appears " + x._2))
  }
}
