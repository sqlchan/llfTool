package spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Seconds

object StreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map((_, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false) // false表示不关闭SparkContext

  }
}
