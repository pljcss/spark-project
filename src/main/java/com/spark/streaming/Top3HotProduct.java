package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 与Spark SQl 整合使用, top3 热门商品实时统计
 */
public class Top3HotProduct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3HotProduct").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 输入日志的格式
         * leo a phone
         * leo a phone
         * leo a phone
         * leo b phone
         * leo c phone
         * leo c phone
         * leo a phone
         */
        // 获取数据源
        JavaReceiverInputDStream<String> productClickLogDStream = jssc.socketTextStream("localhost", 9988);

        /**
         * 映射为 (category_product, 1) 这种格式
         * 后面可以使用window 操作, 对窗口中的这种格式的数据进行 reduceByKey操作
         * 从而统计出, 一个窗口中的每个种类的每个商品的点击次数
         */
        JavaPairDStream<String, Integer> categoryProductPairDStream = productClickLogDStream.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String productClickLog) throws Exception {
                String[] productClickLogSplited = productClickLog.split(" ");
                return new Tuple2<>(productClickLogSplited[2] + "_" + productClickLogSplited[1], 1);
            }
        });

        /**
         * 执行 window 操作
         * 此时, 就可以做到每个10秒, 对最近 60秒的数据执行 reduceByKey操作
         * 计算出这60秒, 每个种类的每个商品的点击次数
         */
        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        /**
         * 然后针对这60秒, 每个种类的每个商品的点击次数
         * foreachRDD, 在内部使用Spark SQL执行 top3商品的统计
         *
         */
        categoryProductCountsDStream.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                /**
                 * 将该RDD转换为 JavaRDD<Row>的格式
                 */
                JavaRDD<Row> categoryProductCountsRowRDD = categoryProductCountsRDD.map(new Function<Tuple2<String, Integer>, Row>() {

                    @Override
                    public Row call(Tuple2<String, Integer> categoryProductCount) throws Exception {
                        String category = categoryProductCount._1.split("_")[0];
                        String product = categoryProductCount._1.split("_")[1];
                        Integer counts = categoryProductCount._2;

                        return RowFactory.create(category, product, counts);
                    }
                });

                /**
                 * 然后, 执行DateFrame 转换
                 */
                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));

                StructType structType = DataTypes.createStructType(structFields);

                /**
                 * 后面执行开窗函数, 所以此处应该使用 HiveContext 来执行开窗函数
                 */
//                SQLContext sqlContext = new SQLContext(categoryProductCountsRDD.context());
//                DataFrame categoryProductCountDF = sqlContext.createDataFrame(categoryProductCountsRowRDD, structType);
                HiveContext hiveContext = new HiveContext(categoryProductCountsRDD.context());
                DataFrame categoryProductCountDF = hiveContext.createDataFrame(categoryProductCountsRowRDD, structType);

                /**
                 * 将这60秒内每个种类的每个商品的点击次数的数据, 注册为一个临时表
                 */
                categoryProductCountDF.registerTempTable("product_click_log");

                /**
                 * 执行 sql语句, 统计出每个种类下点击次数排名前三的热门商品
                 * hiveContext
                 */
                String sonSql = "SELECT category, product, click_count, row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank FROM product_click_log";
                DataFrame top3ProductDF = hiveContext.sql("SELECT category, product, click_count FROM ("+ sonSql + ") tmp WHERE rank <= 3");

                /**
                 * 企业应用中,通常将数据保存到 redis、mysqlDB中, 然后配合J2EE系统进行数据的展示、查询、图形报表
                 */
                top3ProductDF.show();

                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
