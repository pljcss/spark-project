package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * action 算子讲解
 */
public class ActionOperation {
    public static void main(String[] args) {
        collect();
    }

    /**
     * 使用reduce实现对集合中的元素进行累加
     *
     * count 算子, 统计 RDD 中有多少个元素
     *
     * take 算子, take操作与collect操作类似,也是从远程集群上获取RDD数据,
     * 但是collect是获取所有数据, 而take是获取前 n 条数据
     */
    private static void reduce() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);

        Integer res = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(res);


        // 关闭资源
        jsc.close();
    }

    /**
     * collect就是将部署在远程的分布式RDD的所有元素拿到driver本地来
     */
    private static void collect() {
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);

        // map 乘以2
        JavaRDD<Integer> doubleRDD = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        /**
         * 不使用foreach action操作,在远程集群上遍历RDD中的元素
         * 而使用collect操作,将分布在远程集群上的 RDD 的数据拉取到本地
         * 注意, 这种方式一般不建议使用,因为如果RDD中的数量比较大的话,那么性能会比较差,因为要
         * 通过网络将数据从远程拉取到本地, 此外还可能发生 OOM
         * 通常还是建议使用foreach来对最后的元素进行处理
         */
        List<Integer> list = doubleRDD.collect();

        System.out.println(list);


        // 关闭资源
        jsc.close();
    }


    private static void saveAsTextFile() {
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);

        /**
         * 直接将RDD中的数据保存到HDFS上
         * 注意, 这里只能指定文件夹, 也就是目录
         */
        javaRDD.saveAsTextFile("hdfs://greentown/...");


        // 关闭资源
        jsc.close();
    }
}
