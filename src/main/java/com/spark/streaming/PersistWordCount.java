package com.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于持久化机制的 wordcount 程序
 */
public class PersistWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PersistWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("hdfs://greentown//test/wc_update");

        // 接着实现 wordcount 逻辑
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9988);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(

                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> states) throws Exception {

                        Integer newValue = 0;

                        if (states.isPresent()) {
                            newValue = states.get();
                        }

                        for (Integer value : values) {
                            newValue += value;
                        }

                        return Optional.of(newValue);
                    }
                });


        /**
         * 每次得到当前所有单词的统计次数后, 将其写入mysql进行持久化存储, 以便于后续的 J2EE 应用程序进行显示
         */
        wordcounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {

                /**
                 * 调用RDD的 foreachPartition() 方法
                 */
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {

                        /**
                         * 给每个 partition 获取一个连接
                         */
                        Connection conn = ConnectionPool.getConnections();

                        System.out.println("88899999" + conn);

                        /**
                         * 遍历partition中的数据, 使用一个连接插入数据库
                         */
                        Tuple2<String, Integer> wordcount = null;
                        while (wordCounts.hasNext()) {
                            wordcount = wordCounts.next();

                            String sql = "insert into wordcount(word, count) values(" + "'" + wordcount._1
                                    + "','" + wordcount._2 + "')";

                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }

                        /**
                         * 用完后, 将连接还回去
                         */
                        ConnectionPool.returnConnection(conn);
                    }
                });

                return null;
            }
        });



        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
