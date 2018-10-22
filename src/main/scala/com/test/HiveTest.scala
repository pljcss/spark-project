package com.test

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveCount")//.setMaster("local")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val df = hiveContext.sql("select count(*) from l_fix_tmp_g where input_day='20181017'")

    df.show()
    println("---------------------")

  }
}
