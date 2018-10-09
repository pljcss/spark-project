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
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Json 数据源
 */
public class JsonDataSource {
    public static void main(String[] args) {
        // 创建SparkConf、JavaSparkContext、SQlContext
        SparkConf conf = new SparkConf().setAppName("JsonDataSource").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // 针对 json 文件, 创建 DataFrame
        DataFrame dataFrame = sqlContext.read().json("/Users/saicao/Desktop/studet.json");

        // 注册临时表
        dataFrame.registerTempTable("students");

        // 查询分数大于 80 的学生的姓名和分数
        DataFrame highScoreStuDF = sqlContext.sql("select name, score from students where score > 80");

        // DataFrame 转 RDD
        List<String> highScoreStuName = highScoreStuDF.javaRDD().map(new Function<Row, String>() {

            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        /**
         * 针对 JavaRDD 创建DataFrame
         */
        List<String> studentsInfoJson = new ArrayList<>();
        studentsInfoJson.add("{\"name\":\"Leo\",\"age\":\"22\"}");
        studentsInfoJson.add("{\"name\":\"Tom\",\"age\":\"26\"}");
        studentsInfoJson.add("{\"name\":\"Bob\",\"age\":\"28\"}");
        JavaRDD<String> studentInfoRDD = jsc.parallelize(studentsInfoJson);

        DataFrame studentInfoDF = sqlContext.read().json(studentInfoRDD);

        // 针对学生基本信息, 注册临时表
        studentInfoDF.registerTempTable("student_info");

        String sql = "select name, age from student_info where name in (";

        for (int i=0; i<highScoreStuName.size(); i++) {
            sql += "'" + highScoreStuName.get(i) + "'";
            if (i < highScoreStuName.size() -1) {
                sql += ",";
            }
        }
        sql += ")";

        System.out.println(sql);

        DataFrame goodStudentInfoDF = sqlContext.sql(sql);


        goodStudentInfoDF.show();

        /**
         * 将两份 DataFrame 数据转换为 JavaPairRDD
         */
        JavaPairRDD<String, String> studentScoreJavaRDD = highScoreStuDF.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getString(1));
            }
        });

        JavaPairRDD<String, String> studentInfoJavaRDD = goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getString(1));
            }
        });

        // 将两个 RDD 进行 join
        JavaPairRDD<String, Tuple2<String, String>> studentScoreInfo = (JavaPairRDD<String, Tuple2<String, String>>) studentScoreJavaRDD.join(studentInfoJavaRDD);

        studentScoreInfo.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, String>> s) throws Exception {
                System.out.println(s._1 + ", " + s._2);
            }
        });

        // 将 JavaPairRDD 转为 DataFrame
        JavaRDD<Row> scoreInfoRowRDD = studentScoreInfo.map(new Function<Tuple2<String, Tuple2<String, String>>, Row>() {

            @Override
            public Row call(Tuple2<String, Tuple2<String, String>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        });

        // 创建一份元数据将 JavaRDD<Row> 转换为 DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.StringType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame studentScoreInfoDF = sqlContext.createDataFrame(scoreInfoRowRDD, structType);

        // 将好学生的信息保存到一个 json 文件中
        studentScoreInfoDF.write().format("json").save("/Users/saicao/Desktop/studentScoreInfo.json");


        jsc.close();
    }
}
