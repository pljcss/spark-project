package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 * 1、实现自定义的key,要实现Ordered和Serializable接口,在key中实现自己对对多个列的排序算法
 * 2、将包含文本的RDD,映射为key为自定义的key,value为文本的JavaPairRDD
 * 3、使用sortByKey算子对自定义的key进行排序
 * 4、再次映射,剔除自定义的key,只保留文本行
 *
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("/Users/saicao/Desktop/word1.txt");

        JavaPairRDD<SecondarySortKey, String> pair = linesRDD.mapToPair(new PairFunction<String, SecondarySortKey, String>() {

            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] lineSplit = s.split(" ");
                SecondarySortKey key = new SecondarySortKey(
                        Integer.valueOf(lineSplit[0]),
                        Integer.valueOf(lineSplit[1]));

                return new Tuple2<>(key, s);
            }
        });

        /**
         * 打印
         */
        pair.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, String> t) throws Exception {
                System.out.println(t._1.getFirst() + " " + t._1.getSecondary() + ", " + t._2);
            }
        });
        System.out.println("-------- 1 --------");


        /**
         * 此处会按照key进行排序,而key是SecondarySortKey, 是自定的key,
         * 因此排序规则会按照SecondarySortKey定义的规则进行排序
         */
        JavaPairRDD<SecondarySortKey, String> sortedPair = pair.sortByKey();

        /**
         * 打印
         */
        sortedPair.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, String> t) throws Exception {
                System.out.println(t._1.getFirst() + " " + t._1.getSecondary() + ", " + t._2);
            }
        });
        System.out.println("-------- 2 --------");

        JavaRDD<String> sortedLines = sortedPair.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        // 关闭资源
        jsc.close();
    }
}
