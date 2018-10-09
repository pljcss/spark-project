package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * 广播变量
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        final int factor = 2;

        /**
         * java版本创建共享变量调用 JavaSparkContext 的broadcast方法
         */
        Broadcast<Integer> broadcastFactor = jsc.broadcast(factor);

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> numbers = jsc.parallelize(list);

        /**
         * 让rdd中的每个元素都乘以外部定义的factor
         */
        JavaRDD<Integer> multiNumbers = numbers.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                /**
                 * 获取共享变量的值, 使用value()方法
                 */
                int factor = broadcastFactor.value();
                return v1 * factor;
            }
        });

        multiNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        // 关闭资源
        jsc.close();
    }
}
