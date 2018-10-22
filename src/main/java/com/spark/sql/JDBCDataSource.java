package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 数据源
 * 首先, 通过SQLContext 的read 方法, 将mysql的中的数据加载为 DataFrame
 * 然后可以将DataFrame 转换为RDD, 使用Spark Core提供的各种算子进行操作
 * 最后可以将得到的数据, 通过foreach 算子写入 mysql、hbase、redis等db,或cache中
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        /**
         * 分别将mysql 中的两种表加载为 DataFrame
         */
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/test");
        options.put("dbtable", "studens_info");
        options.put("user", "root");
        options.put("password", "gt123");


        DataFrame studentInfoDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "studens_score");
        DataFrame studentScoreDF = sqlContext.read().format("jdbc").options(options).load();

        studentScoreDF.show();

        // 将两个 DataFrame 转换为 JavaPairRDD, 执行join操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentRDD = (JavaPairRDD<String, Tuple2<Integer, Integer>>) studentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        // 将JavaPairRDD 转换为JavaRDD<Row>
        JavaRDD<Row> studentRowsRDD = studentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        });

        // 过滤分数大于80 分的学生
        JavaRDD<Row> filteredStuRowsRDD = studentRowsRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                System.out.println("*****" + row.getInt(2));
                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;

            }
        });


        List<Row> collect = filteredStuRowsRDD.collect();
        for (Row row : collect) {
            System.out.println(row.get(0) + "," + row.get(1) + "," + row.get(2));
        }
        System.out.println("-------------------------------");

        // 转换为 DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame studentsDF = sqlContext.createDataFrame(filteredStuRowsRDD, structType);

        // 将DataFrame 中的数据保存到 mysql
//        options.put("dbtable", "good_studens");
//        studentsDF.write().format("jdbc").options(options).save();

        // 企业常用
        studentsDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String sql = "insert into good_studens values('"
                        + row.getString(0) + "',"
                        + Integer.valueOf(row.getInt(1)) + ","
                        + Integer.valueOf(row.getInt(2)) + ")";

                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement stmt = null;

                try {
                    conn = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306/test",
                            "root",
                            "gt123");
                    stmt = conn.createStatement();
                    stmt.executeUpdate(sql);

                } catch (Exception e) {
                    e.printStackTrace();

                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        });


    }
}
