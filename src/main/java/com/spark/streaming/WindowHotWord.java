package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * 基于滑动窗口的热点搜索词实时统计
 */
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 搜索日志的格式
         * leo news
         * tom foods
         * bob weather
         */
        JavaReceiverInputDStream<String> searchLogDStream = jssc.socketTextStream("localhost", 9988);

        // 将日志转换为一个搜索词
        JavaDStream<String> searchWordDStream = searchLogDStream.map(new Function<String, String>() {

            @Override
            public String call(String searchLog) throws Exception {
                return searchLog.split(" ")[1];
            }
        });

        // 将搜索词映射为 (searchWord, 1) 的 tuple 格式
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        /**
         * 针对 (searchWord, 1) 这种 tuple 格式的DStream, 执行 reduceByKeyAndWindow 滑动窗口操作
         * 第二个参数是 窗口长度
         * 第三个参数是 滑动间隔
         * 即每隔 10秒钟 将最近 60秒 的数据作为一个窗口, 进行内部的RDD聚合, 然后统一对一个RDD进行后续的计算,
         *
         * 所以, 到 searchWordPairDStream 为止, 其实都是不会立即进行计算的, 然后等待滑动间隔到了10秒后,
         * 会将之前的60秒的RDD, 因为一个batch的间隔是5秒, 所以会将之前的 12 个RDD聚合起来, 统一执行 reduceByKey 操作,
         * 所以, 这里的 reduceByKeyAndWindow是针对窗口执行计算的, 而不是针对某个 DStream 的RDD
         */
        JavaPairDStream<String, Integer> searchWordCountsDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        /**
         * 到此为止, 就是每个10秒统计出来之前60秒收集到的单词的统计次数
         *
         * 执行 transform操作, 因为一个窗口就是一个 60秒的数据, 会变成一个RDD, 然后针对这一个RDD根据搜索词出现的
         * 频率进行排序, 然后获取排名前三的热点搜索词
         */
        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {

                    @Override
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {
                        // 执行搜索词和出现频率的反转
                        JavaPairRDD<Integer, String> countSearchWordRDD = searchWordCountsRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

                            @Override
                            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                                return new Tuple2<>(tuple2._2, tuple2._1);
                            }
                        });

                        // 然后执行降序排序
                        JavaPairRDD<Integer, String> sortedCountSearchWordRDD = countSearchWordRDD.sortByKey(false);

                        // 然后再次执行反转
                        JavaPairRDD<String, Integer> sortedSearchCountsRDD = sortedCountSearchWordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                                return new Tuple2<>(tuple2._2, tuple2._1);
                            }
                        });

                        // 然后用 take 获取排名前三的热点搜索词
                        List<Tuple2<String, Integer>> hotSearchWords = sortedSearchCountsRDD.take(3);

                        System.out.println("热点词" + hotSearchWords);

                        return searchWordCountsRDD;
                    }
                });

        /**
         * 这个无关紧要, 只是为了触发 job 的执行, 所以必须有 output 操作
         */
        finalDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
