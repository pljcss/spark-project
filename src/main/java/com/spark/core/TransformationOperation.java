package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformation 算子详解
 * @author cs
 */
public class TransformationOperation {
    public static void main(String[] args) {
        cogroup();
    }

    /**
     * map算子案例, 将集合中每个元素乘以2
     */
    private static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> rdd = jsc.parallelize(numbers);

        /**
         * 使用map算子,将集合中的每个元素乘以2
         * map算子是对任何类型的RDD都可以调用的
         * 在Java中map算子接收的参数是Function对象,创建的Function对象的第二个泛型参数就是返回的新元素的类型
         * 同时call()方法的返回类型也必须与Function的第二个参数类型一致
         * 在 call()方法内部,就可以对原始RDD中的每个元素进行各种处理和计算, 并返回一个新的元素
         */
        JavaRDD<Integer> rddMulti2 = rdd.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        rddMulti2.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("result  " + integer);
            }
        });

        /**
         * 关闭资源
         */
        jsc.close();
    }

    /**
     * filter 算子
     * 过滤出集合中的偶数
     */
    private static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> rdd = jsc.parallelize(numbers);

        JavaRDD<Integer> filterRDD = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {

                return v1 % 2 == 0;
            }
        });

        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.close();
    }


    /**
     * flatMap算子
     * 将文本行拆分为多个单词
     */
    private static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> linesList = Arrays.asList("hello me", "hello you", "hello world");

        JavaRDD<String> lines = jsc.parallelize(linesList);

        /**
         * 对RDD执行flatMap算子,将每一行文本拆分为多个单词
         * 在java中flatMap()接收的参数类型是FlatMapFunction,FlatMapFunction第二个参数代表返回的新元素的类型
         * flatMap()其实就是接收原始RDD中的每个元素,进行处理后,可以返回多个元素
         * 多个元素即封装在 Iterable 集合中,可以使用ArrayList等集合
         * 新的RDD封装了所有的新元素,也就是说,新的RDD的大小一定是 >= 原始RDD的大小
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        /**
         * 关闭资源
         */
        jsc.close();
    }

    /**
     * groupByKey算子, 按照班级进行分组
     */
    private static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1",60),
                new Tuple2<>("class2",80),
                new Tuple2<>("class4",50),
                new Tuple2<>("class2",20));

        // 并行化集合, 创建JavaPairRDD
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(scores);

        /**
         * 执行groupByKey算子, 对每个班级的成绩进行分组
         * groupByKey算子, 返回的还是 JavaPairRDD
         * 但是, JavaPairRDD的第一个泛型类型不变, 第二个泛型类型变成了 Iterable 这种集合类型
         * 也就是说, 按照key进行分组, 每个key可能都会有多个value, 此时多个value 聚合成了 Iterable
         * 接下来就可以方便的处理分组数据了
         */
        JavaPairRDD<String, Iterable<Integer>> groupScores = pairRDD.groupByKey();

        groupScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);

                Iterator<Integer> ite = t._2.iterator();
                while (ite.hasNext()) {
                    Integer it = ite.next();
                    System.out.println(it);
                }
            }
        });

        /**
         * 关闭资源
         */
        jsc.close();
    }

    /**
     * reduceByKey
     * 按班级聚合,统计总分
     */
    private static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1",60),
                new Tuple2<>("class2",80),
                new Tuple2<>("class4",50),
                new Tuple2<>("class2",20));

        // 并行化集合, 创建JavaPairRDD
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(scores);

        /**
         * reduceByKey接收的参数是Function2类型,有三个泛型参数,代表了三个值
         * 第一个和第二个泛型类型代表原始RDD中元素的value类型,第三个代表每次reduce操作返回值的类型
         */
        JavaPairRDD<String, Integer> rdd = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + "  " + tuple2._2);
            }
        });

        // 关闭资源
        jsc.close();
    }

    /**
     * sortByKey 算子
     * 按照学生分数进行排序
     */
    private static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> scores = Arrays.asList(
                new Tuple2<>(60,"Leo"),
                new Tuple2<>(70,"Bob"),
                new Tuple2<>(45,"Micle"),
                new Tuple2<>(99,"LiMeiMei"));

        JavaPairRDD<Integer, String> scoresRDD = jsc.parallelizePairs(scores);

        // 升序
        JavaPairRDD<Integer, String> scoresSortRDD = scoresRDD.sortByKey();
        // 降顺
//        JavaPairRDD<Integer, String> scoresSortRDD = scoresRDD.sortByKey(false);

        scoresSortRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + "  " + t._2);
            }
        });

        // 关闭资源
        jsc.close();
    }

    /**
     * join
     * 进行rdd join
     */
    private static void join() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 模拟学生集合
        List<Tuple2<Integer,String>> studentList = Arrays.asList(
                new Tuple2<>(1, "Leo"),
                new Tuple2<>(2, "Tom"),
                new Tuple2<>(3, "Jack")
        );
        // 模拟分数集合
        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 55),
                new Tuple2<>(2, 98),
                new Tuple2<>(3, 33)
        );

        // 并行化两个RDD
        JavaPairRDD<Integer, String> studentRDD = jsc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scoreList);

        /**
         * 使用join算子关联两个RDD
         * join算子会根据key进行join, 并返回JavaPairRDD
         *  , 第一个是
         */
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentRDD.join(scoresRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println(t._1 + ", " + t._2._1 + ", " + t._2._2);
            }
        });

        // 关闭资源
        jsc.close();
    }

    /**
     * cogroup算子
     * 进行rdd join
     */
    private static void cogroup() {
        SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 模拟学生集合
        List<Tuple2<Integer,String>> studentList = Arrays.asList(
                new Tuple2<>(1, "Leo"),
                new Tuple2<>(2, "Tom"),
                new Tuple2<>(3, "Jack")
        );
        // 模拟分数集合
        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 55),
                new Tuple2<>(2, 98),
                new Tuple2<>(3, 33),
                new Tuple2<>(1, 55),
                new Tuple2<>(2, 98),
                new Tuple2<>(3, 33)
        );

        // 并行化两个RDD
        JavaPairRDD<Integer, String> studentRDD = jsc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scoreList);

        /**
         * cogroup 与 join 不同
         * 相当于一个key
         */
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> joinRDD = studentRDD.cogroup(scoresRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1 + ", " + t._2._1 + ", " + t._2._2);
            }
        });

        // 关闭资源
        jsc.close();
    }
}
