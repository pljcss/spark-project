package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用 json 文件创建DataFrame
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame df = sqlContext.read().json("/Users/saicao/Desktop/json.txt");

        df.show();

        jsc.close();
    }
}
