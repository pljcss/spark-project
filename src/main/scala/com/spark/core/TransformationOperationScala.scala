package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * transformation 算子详解
  * @author cs
  */
object TransformationOperationScala {
  def main(args: Array[String]): Unit = {
    join()
  }

  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5)

    val rdd = sc.parallelize(numbers)

    val rddRes = rdd.map(x=>x*2)

    rddRes.foreach(x=>println(x))
  }

  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val linesList = Array("hello me", "hello you", "hello world")

    val words = sc.parallelize(linesList)

    val rddRes = words.flatMap(x=>x.split(" "))

    rddRes.foreach(x=>println(x))
  }

  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 60),
                          Tuple2("class2", 80),
                          Tuple2("class4", 50),
                          Tuple2("class2", 20))

    val rdd = sc.parallelize(scoreList)

    val groupScores = rdd.groupByKey()

    groupScores.foreach(scores=>{
      println(scores._1)
      scores._2.foreach(singleScore => println(singleScore))
    })
  }

  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 60),
      Tuple2("class2", 80),
      Tuple2("class4", 50),
      Tuple2("class2", 20))

    val rdd = sc.parallelize(scoreList)

    val rddRes = rdd.reduceByKey(_+_)

    rddRes.foreach(x=>println(x._1 + "  " + x._2))
  }

  def sortByKey(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2(60,"Bob"),
      Tuple2(80, "Lily"),
      Tuple2(50, "Micle"),
      Tuple2(20, "HanMeiMei"))

    val rdd = sc.parallelize(scoreList)

    val rddRes = rdd.sortByKey()

    rddRes.foreach(x=>println(x._1 + "  " + x._2))
  }


  def join(): Unit = {
    val conf = new SparkConf().setAppName("join").setMaster("local")
    val sc = new SparkContext(conf)

    // 模拟学生集合
    val studentList = Array(
        Tuple2(1, "Leo"),
        Tuple2(2, "Tom"),
        Tuple2(3, "Jack"))

    // 模拟分数集合
    val scoreList = Array(
        Tuple2(1, 55),
        Tuple2(2, 98),
        Tuple2(3, 33),
      Tuple2(1, 55),
      Tuple2(2, 98),
      Tuple2(3, 33))

    val stuRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)

    val rdd: RDD[(Int, (String, Int))] = stuRDD.join(scoreRDD)

    rdd.foreach(x=>{
//      println(x._1)
//      println(x._2._1)
//      println(x._2._2)

//      println(x._1, x._2._1, x._2._2)
      println(x._1 + ", " + x._2._1 + ", " + x._2._2)


    })
  }

  def cogroup(): Unit = {
    val conf = new SparkConf().setAppName("cogroup").setMaster("local")
    val sc = new SparkContext(conf)

    // 模拟学生集合
    val studentList = Array(
      Tuple2(1, "Leo"),
      Tuple2(2, "Tom"),
      Tuple2(3, "Jack"))

    // 模拟分数集合
    val scoreList = Array(
      Tuple2(1, 55),
      Tuple2(2, 98),
      Tuple2(3, 33),
      Tuple2(1, 55),
      Tuple2(2, 98),
      Tuple2(3, 33))

    val stuRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)

    val rdd: RDD[(Int, (Iterable[String], Iterable[Int]))] = stuRDD.cogroup(scoreRDD)

    rdd.foreach(x=>{
      println(x._1 + ", " + x._2._1 + ", " + x._2._2)
    })
  }


}
