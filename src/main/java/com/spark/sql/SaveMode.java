package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SaveMode {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("SaveMode").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // 使用 format() 指定读的数据类型
        DataFrame df = sqlContext.read().format("json").load("/Users/saicao/Desktop/jj/json.txt");

        // 使用 SaveMode, 如果目标文件存在, 则追加
        df.save("/Users/saicao/Desktop/jj", "json", org.apache.spark.sql.SaveMode.Append);


        jsc.close();
    }
}
