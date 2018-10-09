package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * parquet 数据源之使用编程方式加载数据
 */
public class ParquetLoadData {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("SaveMode").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // 读取 parquet 文件中的数据, 创建一个 DataFrame
        DataFrame userDF = sqlContext.read().parquet("/Users/saicao/Desktop/users.parquet");

        // 将 DataFrame 注册为临时表, 并使用 SQL 查询数据
        userDF.registerTempTable("users");
        DataFrame userNameDF = sqlContext.sql("select name from users");

        // 对查询出来的DataFramej进行transformation操作
        JavaRDD<String> userRDD = userNameDF.javaRDD().map(new Function<Row, String>() {

            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        });

        List<String> list = userRDD.collect();

        System.out.println(list);

        jsc.close();
    }
}
