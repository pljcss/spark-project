package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySortScala").setMaster("local")
    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("/Users/saicao/Desktop/word1.txt")

    val pairs = linesRDD.map(line => (new SecondarySortKeyScala(line.split(" ")(0).toInt, line.split(" ")(1).toInt),line))

    val sortedPairs = pairs.sortByKey()

    val sortedLines = sortedPairs.map(x => x._2)

    sortedLines.foreach(x => println(x))
  }
}
