package maizi

import org.apache.spark.sql.SparkSession


object KuduSparkDev {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Kudu")
      .master("local")
      .getOrCreate()


    val kuduDF = spark.read.format("org.apache.kudu.spark.kudu").
      option("kudu.master", "dev-bg-02:7051").
      option("kudu.table", "kudu_test.decimal_test").
      option("kudu.table", "impala::kudu_test.kudu_app_click_log")
      .load()

    kuduDF.show(3)
    // register a temporary table and use SQL
    kuduDF.createOrReplaceTempView("kudu_table")

    val filteredDF = spark.sql("select * from kudu_table").show()
  }
}
