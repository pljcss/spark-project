package com.scala.test

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List("coffee panda","happy panda","happiest panda party"))

    rdd.map(x=>x.split(" ")).foreach(x=>{
      x.foreach(x=>println(x))
    })

//    rdd.flatMap(x=>x.split(" ")).foreach(x=>println(x))
  }
}
