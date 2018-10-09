package com.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于 transform 的实时黑名单过滤
 */
public class TransformBlacklist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 用户点击网站上的广告,正常情况点击一次, 算一次钱
         * 但是对于某些无良商家刷广告的人, 我们需要设置一个黑名单,
         * 只要是黑名单中的用户点击广告, 我们就过滤掉
         */

        /**
         * 模拟黑名单 RDD
         * true 代表黑名单是启用状态
         */
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
        blacklist.add(new Tuple2<>("tom", true));
        // final
        final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sc().parallelizePairs(blacklist);

        /**
         * 这里的日志格式简化为
         * date username 的格式
         * 20180901 tom
         * 20180901 leo
         */
        JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("localhost", 9988);

        /**
         * 所以, 需要先对输入的数据进行转换操作, 变为 (username, date username)
         * 以便后面对每个 batch RDD 与定义好的黑名单进行 join 操作
         */
        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {

            @Override
            public Tuple2<String, String> call(String adClickLog) throws Exception {
                return new Tuple2<String, String>(adClickLog.split(" ")[1], adClickLog);
            }
        });

        /**
         * 然后执行 transform 操作, 将每个 batch 的RDD 与黑名单 RDD进行 join、filter、map等操作
         * 实时进行黑名单过滤
         */
        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(
                new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLog) throws Exception {

                        /**
                         * 使用左外连接
                         * 因为, 并不是每个用户都存在于黑名单中的, 所以如果直接使用 join, 那么没有存在与黑名单中的数据会无法join 到
                         * 就被丢弃了, 使用leftOuterJoin ,当user不在黑名单RDD中,没有join 到也会保存下来
                         */
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
                                (JavaPairRDD<String, Tuple2<String, Optional<Boolean>>>) userAdsClickLog.leftOuterJoin(blacklistRDD);


                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinedRDD.filter(
                                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                                // 这里的 tuple 就是每个用户对应的访问日志, 和在黑名单中的状态
                                if (tuple._2._2().isPresent() && tuple._2._2.get()) {
                                    return false;
                                }
                                return true;
                            }
                        });


                        /**
                         * 此时, filterRDD 中只剩下了不在黑名单中的用户了
                         * 进行map操作, 转换为我们需要的格式
                         */
                        JavaRDD<String> validAdsClickLogRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                            @Override
                            public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                                return tuple._2._1;
                            }
                        });

                        return validAdsClickLogRDD;
                    }
                });

        /**
         * 打印有效的广告点击日志
         * 其实在实际企业场景中, 这里就可以写入Kafka、ActiveMQ等这种中间件消息队列
         * 然后在开发一个专门的后台服务, 作为广告计费服务, 执行实时的广告计费
         */
        validAdsClickLogDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
