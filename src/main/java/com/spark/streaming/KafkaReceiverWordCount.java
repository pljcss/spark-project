package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 数据源
 * 基于 Receiver 方式
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 使用 KafkaUtils.createStream()方法, 创建针对Kafka的输入数据流
         * 接收到的值是pair, 但第一个值通常为null
         * createStream 第二个参数为 zookeeper 连接地址,ip:port
         * 第三个参数是 consumer 的 group id, 随意给值
         * 第四个参数是, 指定每个 topic 用几个线程去并发拉取 partition 里的数据
         */
        Map<String, Integer> topicThread = new HashMap<>(5);
        topicThread.put("WordCount", 1);

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc, "10.0.0.0:2181, ", "DefaultGroup", topicThread);

        // 开发 WordCount 逻辑
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

            @Override
            public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
