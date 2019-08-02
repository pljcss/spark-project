package maizi
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory


object SparkStreaming2Demo {

  val logger = LoggerFactory.getLogger(SparkStreaming2Demo.getClass)

  def main(args: Array[String]): Unit = {

    // 设置打印日志级别
//    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    // Create a local StreamingContext with two working thread and batch interval of 5 second
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

//    ssc.sparkContext.setLogLevel("WARN")
//    ssc.sparkContext.setLogLevel("ERROR")

    logger.info("开始消费数据------")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    lines.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
