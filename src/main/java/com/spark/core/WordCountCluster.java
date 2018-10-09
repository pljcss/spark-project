package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 集群运行 wordcount 程序
 */
public class WordCountCluster {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 读取 HDFS 数据
         */
        JavaRDD<String> lines = sc.textFile("hdfs://greentown/wordcount_dir/wordcount.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {

                System.out.println(wordcount._1 + "出现了" + wordcount._2);
            }
        });

        // 关闭资源
        sc.close();
    }
}
