package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取 topN
  * 原始文件是单列值
  * 1
  * 5
  * 2...
  * sortByKey对key进行排序,需要是tuple类型
  */
object Top3Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/Users/saicao/Desktop/word2.txt")

    val pair = rdd.map(x=>(x.toInt,x))

    val sortRDD = pair.sortByKey(false)

    val res = sortRDD.map(x=>x._1)

    val top3 = res.take(3)

    for (x <- top3) println(x)
  }
}
