package com.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 基于 updateStateByKey 算子实现缓存机制的 wordcount 程序
 */
public class UpdateStateByKeyWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWC").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 第一, 如果要使用 updateStateByKey 算子, 就必须设置一个 checkpoint 目录,启用 checkpoint 机制
         * 每个key存在于state的内存中, 也需要checkpoint一份长期保存, 以便内存中的数据丢失的时候, 可以从 checkpoint
         * 中恢复数据
         *
         * 开启 checkpoint 很简单, 只需要调用 jssc 的 checkpoint 方法, 设置一个 hdfs 目录即可
         */
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

        // 只会执行一次
        pairs.print();
        System.out.println("----------------------");
        /**
         * 此处, 如果直接使用 reduceByKey 算子,需要基于redis、mysql 这种db来实现累加操作
         * 但是 updateStateByKey 就可以通过spark维护一份每个单词的全局统计次数
         */
        JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(

                /**
                 * 这里的 Optional 相当于 Scala 中的样例类, 就是它代表了一个值的存在状态, 可能存在也可能不存在
                 */
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    /**
                     * 每次 batch 计算的时候, 都会调用这个函数
                     * 第一个参数 values, 相当于是在这个 batch 中, 这个key的新的值,可能有多个,
                     * 比如说一个 hello, 可能有两个 (hello, 1)、(hello, 1), 那么传入的就是 (1,1)
                     * 第二个参数就是指这个key之前的状态, 其中的泛型类型是你自己指定的
                     * @param values
                     * @param state
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        /**
                         * 首先,定义一个全局的单词计数
                         */
                        Integer newValue = 0;

                        /**
                         * 其次,判断 state 是否存在,如果不存在
                         * 如果不存在,说明这个key第一次出现
                         * 如果存在, 说明这个key之前统计过全局次数了
                         */
                        if (state.isPresent()) {
                            newValue = state.get();
                        }

                        /**
                         * 接着, 将本次新出现的值都累加到 newValue 上去, 就是一个 key 目前的全局统计次数
                         */
                        for (Integer value : values) {
                            newValue += value;
                        }

                        return Optional.of(newValue);
                    }
                });

        /**
         * 到此为止, 每个batch过来都会执行 updateStateByKey 算子, updateStateByKey 返回
         * JavaPairDStream 代表每个key的全局统计次数
         */
        wordcounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
