package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object HiveDataSourceScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveDataSourceScala").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    // stu_info 表
    hiveContext.sql("DROP TABLE IF EXISTS stu_info")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS stu_info(name string, age int) row format delimited fields terminated by '\\t'")
    hiveContext.sql("load data local inpath '/root/stu_info.txt' into table stu_info")

    // stu_score 表
    hiveContext.sql("DROP TABLE IF EXISTS stu_score")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS stu_score(name string, score int) row format delimited fields terminated by '\\t'")
    hiveContext.sql("load data local inpath '/root/stu_score.txt' into table stu_score")


    val goodStuDF = hiveContext.sql("select si.name, si.age, ss.score from stu_info si " + "left join stu_score ss on si.name=ss.name where ss.score>80")
    // 将DF中的数据保存到 good_stu 表中
    hiveContext.sql("DROP TABLE IF EXISTS good_stu")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS good_stu(name string, age int, score int)")
    goodStuDF.saveAsTable("good_stu")

    val goodStuRows = hiveContext.table("good_stu").collect

    for (row <- goodStuRows) {
      System.out.println(row)
    }
  }
}
