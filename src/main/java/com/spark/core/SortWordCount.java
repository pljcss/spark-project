package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * wordcount排序
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("/Users/saicao/Desktop/word.txt");

        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });


        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // key value反转
        JavaPairRDD<Integer, String> valueKeyRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        });

        JavaPairRDD<Integer, String> sortRDD = valueKeyRDD.sortByKey(false);

        JavaPairRDD<String, Integer> rdd = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ", " + tuple2._2);
            }
        });

//        rdd.saveAsTextFile("/Users/saicao/Desktop/file2.txt");
        // 关闭资源
        jsc.close();
    }
}
