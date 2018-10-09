package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 */
public class PersistRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PersistRDD").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        /**
         * cache()或persist()的使用是有规则的
         * 必须在transformation或者textFile等创建了一个RDD之后,直接连续调用cache()或persist()才可以
         *
         * 如果你先创建一个RDD,然后单独另起一行执行cache()或persist()操作是没有用的
         * 而且会报错,大量文件丢失
         */
        JavaRDD<String> rdd = jsc.textFile("/Users/saicao/Desktop/file1.txt");

        long startTime = System.currentTimeMillis();

        long linesCounts = rdd.count();
        System.out.println(linesCounts);

        long endTime = System.currentTimeMillis();

        // rdd cost 2151 milliseconds
        System.out.println("rdd cost " + (endTime - startTime) + " milliseconds");

        long startTime1 = System.currentTimeMillis();

        long linesCounts1 = rdd.count();
        System.out.println(linesCounts);

        long endTime1 = System.currentTimeMillis();

        // rdd1 cost 1596 milliseconds
        System.out.println("rdd1 cost " + (endTime1 - startTime1) + " milliseconds");


        // 关闭资源
        jsc.close();
    }
}
