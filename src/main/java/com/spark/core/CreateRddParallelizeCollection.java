package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class CreateRddParallelizeCollection {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf().setAppName("CreateRddParallelizeCollection").setMaster("local");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 通过并行化集合方式创建RDD
        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> rdd = sc.parallelize(list);

        // 执行reduce算子, java中无法使用rdd.reduce(_+_),scala函数式编程的优点
        Integer nums = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(nums);

        // 关闭JavaSparkContext
        sc.close();

    }
}
