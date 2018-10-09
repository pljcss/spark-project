package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive 数据源
 */
public class HiveDataSource {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("HiveDataSource").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 创建 HiveContext, 注意, 这里接收的是 SparkContext 参数, 而是不 JavaSparkContext
        HiveContext hiveContext = new HiveContext(jsc.sc());

        jsc.close();
    }
}
