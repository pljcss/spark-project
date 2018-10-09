package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 */
public class ManuallySpecifyOption {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // 使用 format() 指定读的数据类型
        DataFrame df = sqlContext.read().format("json").load("/Users/saicao/Desktop/json.txt");

        df.show();

        // 使用 format() 指定写的数据类型
        df.select("name").write().format("parquet").save("/Users/saicao/Desktop/jsonParquet.parquet");

        jsc.close();
    }
}
