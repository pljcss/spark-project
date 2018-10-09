package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式动态指定元数据, 将 RDD 转换为 DataFrame
 */
public class Rdd2DataFrameProgrammatically {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("Rdd2DataFrameProgrammatically").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        /**
         * 第一步, 创建普通RDD, 但是必须将其转换为 RDD<Row> 这种格式
         */
        JavaRDD<String> lines = jsc.textFile("/Users/saicao/Desktop/student.txt");
        // 往 Row 里塞数据的时候, 需要转换成执行格式, 否则使用的时候会报错
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] lineSplit = line.split(",");
                return RowFactory.create(Integer.valueOf(lineSplit[0]), lineSplit[1], Integer.valueOf(lineSplit[2]));
            }
        });

        /**
         * 第二步, 动态构造元数据
         * 比如说, id、name等, field的名称和类型,可能都是在程序运行过程中,动态从mysql db里
         * 或者是配置文件中加载出来的, 是不固定的
         * 所以特别适合用这种编程的方式, 来构造元数据
         */
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fieldList);

        /**
         * 第三步, 使用动态构造的元数据, 将 RDD 转换为 DataFrame
         */
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD, structType);

        // 后面就可以使用 DataFrame 了
        // 注册临时表
        studentDF.registerTempTable("students");

        DataFrame resDF = sqlContext.sql("select * from students where age > 30");

        resDF.show();

        jsc.close();

    }
}
