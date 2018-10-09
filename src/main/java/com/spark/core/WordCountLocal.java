package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountLocal {
    public static void main(String[] args) {

        /**
         * 编写Spark程序
         * 第一步,
         * 创建SparkConf对象,设置Spark应用的配置信息
         * 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url,
         * 如果设置为 local 则表示本地运行
         */
        SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

        /**
         * 第二步,
         * 创建 JavaSparkContext 对象
         * 在 spark 中,SparkContext是所有功能的一个入口, 无论是java、scala、python编写都需要有一个SparkContext,
         * SparkContext的主要作用,包括初始化Spark应用程序所需的一些核心组件,包括调度器(DAGSchedule,TaskSchedule),
         * 还会到Spark Master节点去注册等等...
         * 但是, 在Spark中,编写不同的Spark应用程序,使用的SparkContext是不同的, 如果使用scala, 使用的则是原生的SparkContext对象,
         * 如果使用的是java, 则使用的是JavaSparkContext对象,
         * 如果开发的是SparkSQL程序, 那么使用的就是SQLContext、HiveContext
         * 如果开发的是Spark Streaming程序, 那么使用的就是...
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 第三步,
         * 针对数据源(HDFS文件、本地文件...), 创建一个初始的RDD,
         * 输入源中的数据会被打散, 分配到RDD的每个partition中, 从而形成一个初始的分布式数据集,
         * 此处是本地测试, SparkContext中根据文件类型的输入创建RDD的方法, 叫做textFile()方法,
         * 在java中, 创建的普通RDD都叫 JavaRDD
         */
        JavaRDD<String> lines = sc.textFile("/Users/saicao/Desktop/word.txt");

        /**
         * 第四部,
         * 对初始RDD进行transformation操作, 也就是一些计算操作
         * 通常操作会通过创建function, 并配合RDD的map、flatMap等算子来执行
         * function如果比较简单, 则创建指定Function的匿名内部类,
         * 但是如果function比较复杂, 则会单独创建一个类, 作为实现这个function接口的类
         */

        /**
         * 先将每一行拆分成单个单词
         * FlatMapFunction有两个泛型参数, 分别代表了输入和输出类型
         * 此处输入肯定是String, 因为是一行一行的文本,输出也是String, 因为是每一行的文本
         * flatMap()的作用, 将RDD的一个元素拆分成一个或多个元素
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        /**
         * 接着, 需要将每一个单词映射为 (单词, 1) 这种格式
         * 只有这样, 后面才能根据单词作为 key 来进行每个单词出现次数的累加
         * mapToPair()其实就是将每个元素, 映射为一个(v1,v2)这样的 Tuple2 类型的元素
         * 这里的Tuple2 就是scala类型, 包含了两个值
         * mapToPair()这个算子,要求的是与 PairFunction 配合使用, 第一个泛型参数代表了输入类型、第二个和第三个泛型参数
         * 代表输出的Tuple2的第一个和第二个值的类型
         * JavaPairRDD的两个泛型分别代表了 Tuple2两个元素的值的类型
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * 接着, 需要以单词作为key, 统计每个单词出现的次数
         * 此处使用reduceByKey()这个算子,对每个key对应的value进行reduce操作
         * 比如JavaPairRDD中有几个元素, 分别是(hello,1)、(hello,1)、(world,1)
         * reduce操作相当于把第一个值和第二个值相加,再将结果与第三个值相加
         * 最后返回的 JavaPairRDD 中的元素,也是tuple, 第一个值就是每个key,第二个值就是key的value被reduce之后的结果,
         * 就是每个单词出现的次数
         */
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 到此为止,通过几个简单的spark算子,已经统计了单词的次数
         * 但是,之前我们使用的flatMap、mapToPair、reduceByKey这种操作都叫做 transformation 操作,但是只有transformation
         * 操作是不会执行的,必须有一个action操作, 比如使用 foreach 来触发程序的执行
         */
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {

                System.out.println(wordcount._1 + "出现了" + wordcount._2);
            }
        });

        /**
         * 最后,
         * 关闭资源
         */
        sc.close();
    }
}
