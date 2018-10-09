package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * wordcount 排序
  */
object SortWordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCountScala").setMaster("local")
    val sc = new SparkContext(conf)

    val linesRdd = sc.textFile("/Users/saicao/Desktop/word.txt")

    val rdd = linesRdd.flatMap(_.split(" "))
                      .map(x=>(x,1))
                      .reduceByKey(_+_)
                      .map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1))


    rdd.foreach(x=>println(x))
  }
}
