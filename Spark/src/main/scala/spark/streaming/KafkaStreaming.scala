//package spark.streaming
//
//import org.apache.log4j.Logger
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//object KafkaStreaming {
//  val  logger: Logger = Logger.getLogger(KafkaStreaming.getClass.getName)
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc,Seconds(2))
//
//    val zkQueue = "10.33.57.46:2181"
//    val consumer = "kafkastream"
//    val topicMap = Map("MAC_INFO_TOPIC" -> 1)
//    val lines = KafkaUtils.createStream(ssc,zkQueue,consumer,topicMap)
////    val words = lines.flatMap(_._2.split(" "))
////    val pairs = words.map((_, 1))
////    val wordCounts = pairs.reduceByKey(_ + _)
//    lines.map(_._2).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//  def tostr(o:String): Unit ={
//    println("dddddddddd")
//  }
//}
