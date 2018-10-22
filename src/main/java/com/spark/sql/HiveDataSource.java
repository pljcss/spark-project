package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive 数据源
 */
public class HiveDataSource {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("HiveDataSource").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 创建 HiveContext, 注意, 这里接收的是 SparkContext 参数, 而是不 JavaSparkContext
        HiveContext hiveContext = new HiveContext(jsc.sc());

        /**
         * 功能1, 使用HiveContext 的 sql/hql 方法,可以执行hive中能够执行的hql语句
         */
        // 如果表存在则删除表
        hiveContext.sql("DROP TABLE IF EXISTS stu_info");
        // 建表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS stu_info(name string, age int) row format delimited fields terminated by '\\t'");
        // 导入数据
        hiveContext.sql("load data local inpath '/root/stu_info.txt' into table stu_info");

        // stu_score 表
        hiveContext.sql("DROP TABLE IF EXISTS stu_score");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS stu_score(name string, score int) row format delimited fields terminated by '\\t'");
        hiveContext.sql("load data local inpath '/root/stu_score.txt' into table stu_score");

        /**
         * 查询分数大于 80分的学生信息
         */
        DataFrame goodStuDF = hiveContext.sql("select si.name, si.age, ss.score from stu_info si " +
                "left join stu_score ss on si.name=ss.name where ss.score>80");

        // 将DF中的数据保存到 good_stu 表中
        hiveContext.sql("DROP TABLE IF EXISTS good_stu");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS good_stu(name string, age int, score int)");
        goodStuDF.saveAsTable("good_stu");

        // 从good_stu 表, 直接创建DF
        Row[] goodStuRows = hiveContext.table("good_stu").collect();

        for (Row row : goodStuRows) {
            System.out.println(row);
        }

        jsc.close();
    }
}
