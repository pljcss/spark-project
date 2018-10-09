package com.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加变量
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf  = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        /**
         * 创建 Accumulator 需要调用JavaSparkContext的accumulator方法
         */
        Accumulator<Integer> sum = jsc.accumulator(0);

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = jsc.parallelize(list);

        numbers.foreach(new VoidFunction<Integer>() {
            /**
             * 然后在函数内部,就可以对Accumulator变量调用add方法累加值
             * @param integer
             * @throws Exception
             */
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);

                /**
                 * 此处会报错
                 */
//                System.out.println("----------------");
//                System.out.println(sum.value());
            }
        });

        /**
         * 在driver程序中就可以调用Accumulator的value方法,获取其值
         */
        System.out.println(sum.value());


        // 关闭资源
        jsc.close();

    }
}
