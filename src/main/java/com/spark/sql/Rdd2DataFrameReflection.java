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
 * 使用放射的方式将rdd 转换为 dataframe
 */
public class Rdd2DataFrameReflection {

    public static void main(String[] args) {
        // 创建普通RDD
        SparkConf conf = new SparkConf().setAppName("Rdd2DataFrameReflection").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        JavaRDD<String> lines = jsc.textFile("/Users/saicao/Desktop/student.txt");

        JavaRDD<Student> studentJavaRDD = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] stuArr = line.split(",");

                Student stu = new Student();
                stu.setId(Integer.valueOf(stuArr[0]));
                stu.setName(stuArr[1]);
                stu.setAge(Integer.valueOf(stuArr[2]));

                return stu;
            }
        });

        /**
         * 使用反射方式, 将 RDD 转换为 DataFrame
         * 将 Student.class 传入进去, 其实就是通过反射的方式来创建 DataFrame
         * 因为 Student.class 本身就是反射的一个应用
         * 然后底层还得通过对 Student Class进行反射, 来获取其中的field
         * 这里要求 JavaBean 必须是现实 serializable 接口, 是可序列化的
         */
        DataFrame studentDF = sqlContext.createDataFrame(studentJavaRDD, Student.class);

        /**
         * 拿到了 DataFrame 之后, 就可以将其注册为临时表, 然后针对其中的数据执行 SQL 语句
         */
        studentDF.registerTempTable("students");

        // 对 students 临时表执行sql语句
        DataFrame teenagerDF = sqlContext.sql("select * from students where age > 30");

        // 将查询出来的 DataFrame 再次转换为 RDD
        JavaRDD<Row> rowJavaRDD = teenagerDF.javaRDD();

        // 将 RDD 中的数据再次映射为 Student
        // row 的顺序和我期望的可能不一样
        JavaRDD<Student> studentJavaRDD1 = rowJavaRDD.map(new Function<Row, Student>() {

            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                System.out.println(row.get(0));
                System.out.println(row.get(1));
                System.out.println(row.get(2));

                stu.setAge(row.getInt(0));
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));

                return stu;
            }
        });

        // 将数据 collect 回来, 打印
        List<Student> studentList = studentJavaRDD1.collect();
        System.out.println(studentList);

        jsc.close();
    }
}
