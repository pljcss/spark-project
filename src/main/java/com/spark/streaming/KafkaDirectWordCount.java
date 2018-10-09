package com.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 基于 Kafka Direct 方式的实时 wordcount 程序
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 首先, 要创建一份 Kafka 参数 Map, 存放 kafka broker 节点
        Map<String, String> kafkaParams = new HashMap<>(5);
        kafkaParams.put("metadata.broker.list", "kafkaIp:kafkaPort");

        /**
         * 然后, 要创建一个set, 里面放入你要读取的topic,
         * 可以并行读取多个topic
         */
        Set<String> topics = new HashSet<>();
        topics.add("wordcount");

        /**
         * 创建输入 DStream
         * 第二个参数为 key 的类型
         */
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
        
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
