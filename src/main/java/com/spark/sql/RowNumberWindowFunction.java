package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * row_number() 开窗函数
 * 销售额 分组取 top3
 * 开窗函数执行相对耗时
 * @author css
 */
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(jsc.sc());

        // 创建销售额表 sales
        hiveContext.sql("drop table if exists sales_test");
        hiveContext.sql("create table sales_test(product string, category string, revenue string) row format delimited fields terminated by '\t'");
        hiveContext.sql("load data local inpath '/root/sales_test.txt' overwrite into table sales_test");

        /**
         * 使用 row_number() 开窗函数, 编写业务逻辑
         * row_number()函数的作用, 就是给每个分组的数据, 按照其排序顺序, 打上一个分组内的行号
         * 比如, 有一个分组, date=20151001, 里面有三条数据, 1122、1121、1124
         * 对这个分组的每一行使用 row_number()开窗函数后, 这三行数据依次获得一个组内的行号
         * 行号从1 开始递增, 比如, 1122 1、1121 2、1124 3
         *
         * row_number() 开窗函数语法说明
         * 可以在 select 查询时, 使用 row_number() 函数, 其次row_number()函数后面, 先跟上 over关键字
         * 然后括号中是 partition by, 也就是根据哪个字段进行分组, 其次可以使用 order by进行组内排序
         * 然后 row_number() 就可以给组内的行一个行号
         *
         */
        DataFrame top3SaleDF = hiveContext.sql("select product, category, revenue from(select product, category, revenue, row_number() over (partition by category order by revenue desc) rank from sales_test) tmp_sales where rank<=3");

        // 将每组排名前三的数据, 保存到一个表中
        top3SaleDF.toDF().registerTempTable("top3_sales_tmp");
        hiveContext.sql("create table top3_sales as select * from top3_sales_tmp");

        // 此种方式报错
//        hiveContext.sql("drop table if exists top3_sales");
//        top3SaleDF.saveAsTable("top3_sales");

        jsc.close();

    }
}
