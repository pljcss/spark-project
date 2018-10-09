package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 实时 wordcount 程序
 */
public class WordCountStreaming {
    public static void main(String[] args) throws InterruptedException {

        /**
         * 创建 SparkConf 对象
         * 这里需要设置一个master属性, 但在测试的时候使用 local 模式
         * local后面必须跟着一个方括号, 里面填写一个数字, 数字代表了用几个线程来执行 Spark Streaming 程序
         */
        SparkConf conf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]");

        /**
         * 创建 JavaStreamingContext 对象
         * 该对象就类似于 Spark Core 中的 JavaSparkContext, 就类似于Spark SQl中的 SQL Context
         * 还必须接收一个batch interval参数, 就是说每收集多长时间的数据, 划分为一个batch进行处理
         * 这里设置为一秒
         */
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));


        /**
         * 创建 DStream, 代表了一个从数据源(比如kafka, socket)来的持续不断的实时数据流
         * 调用 JavaStreamingContext 的 socketTextStream()方法, 可以创建一个数据源为Socket网络端口的数据流,
         * JavaReceiverInputDStream 代表了一个输入的 DStream
         * socketTextStream()方法接收两个基本参数, 第一个是监听哪个主机上的端口, 第二个是监听端口
         */
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        /**
         * 至此, 可以理解为 JavaReceiverInputDStream 中, 每隔一秒会有一个RDD, 其中封装了这一秒发送来的数据
         * RDD的元素类型为String, 即一行行的文本
         * 所以, 这里 JavaReceiverInputDStream<String> 的泛型类型<String>, 就代表了它底层RDD的泛型类型
         */

        /**
         * 开始对接收的数据执行Spark Core提供的算子, 执行应用在DStreaming中即可
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String, Integer> wordsPair = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordcounts = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        Thread.sleep(5000);
        wordcounts.print();

        /**
         * 首先对 JavaStreamingContext 进行一下后续处理
         * 必须调用 start()方法, 整个Spark Streaming Application 才会启动
         */
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
