package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每一行出现的次数(相同内容行出现的次数)
 * mapToPair()算子使用
 */
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.setLogLevel("ERROR");

        JavaRDD<String> rdd = jsc.textFile("/Users/saicao/Desktop/word.txt");


        JavaPairRDD<String, Integer> javaPairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> pairRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + "  appears  " + tuple2._2);
            }
        });

        /**
         * 关闭资源
         */
        jsc.close();
    }
}
