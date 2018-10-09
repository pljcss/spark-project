package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 取 top3
 * 原始文件只有一列
 *
 * 44
 * 2
 * 55...
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("/Users/saicao/Desktop/word2.txt");

        JavaPairRDD<Integer, String> pairRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortedRDD = pairRDD.sortByKey(false);

        JavaRDD<String> sortedRDD2 = sortedRDD.map(new Function<Tuple2<Integer,String>, String>() {

            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1.toString();
            }
        });

        // 取 top3
        List<String> top3 = sortedRDD2.take(3);

        System.out.println(top3);


        // 关闭资源
        jsc.close();
    }
}
