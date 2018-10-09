package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * 分组取top3
 */
public class GroupTop3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("GroupTop3").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("/Users/saicao/Desktop/score.txt");

        JavaPairRDD<String, Integer> pairRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();

        /**
         * 打印
         */
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1 + ", " + t._2);
            }
        });
        System.out.println("---------");


        groupRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                Integer[] top3 = new Integer[3];

                String className = t._1;
                Iterator<Integer> scores = t._2.iterator();


                /**
                 * 需实现这部分算法
                 */
                while (scores.hasNext()) {
                    Integer score = scores.next();


                }



                return null;
            }
        });

        // 关闭资源
        jsc.close();
    }


}
