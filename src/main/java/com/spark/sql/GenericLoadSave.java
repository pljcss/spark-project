package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的 load 和 save 操作
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // load 操作
        DataFrame df = sqlContext.read().load("/Users/saicao/Desktop/users.parquet");

        df.show();

        // save 操作
        df.select("name", "favorite_color").write().save("/Users/saicao/Desktop/tt.parquet");

        jsc.close();

    }
}
