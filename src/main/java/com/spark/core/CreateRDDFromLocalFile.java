package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class CreateRDDFromLocalFile {
    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf().setAppName("CreateRddParallelizeCollection").setMaster("local");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/saicao/Desktop/file1.txt");

        JavaRDD<Integer> lengthRdd = rdd.map(new Function<String, Integer>() {

            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        Integer nums = lengthRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(nums);
    }
}
