package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScalaLocal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

    val sc = new SparkContext(conf);

    val lines = sc.textFile("/Users/saicao/Desktop/word.txt");

    val words = lines.flatMap(lines => lines.split(" "));

    val pairs = words.map(word => (word, 1));

    val wordCounts = pairs.reduceByKey(_+_);

    wordCounts.foreach(wordCounts => println(wordCounts._1 + "出现了" + wordCounts._2))

  }
}
