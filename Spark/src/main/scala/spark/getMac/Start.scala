//package spark.getMac
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//
//object Start {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc,Seconds(5))
//    val cache: mutable.HashMap[String, RDD[TerminalInfo]] = mutable.HashMap.empty[String, RDD[TerminalInfo]]
//
//    val macStream = KafkaUtils.createStream(ssc,"10.33.57.46:2181","llfmac1",Map("MAC_INFO_TOPIC" -> 1)).map(_._2).map(x => {
//      new FormatMAC().handle(x)
//    }).filter(x => null != x )
//
//
//    resolveMacTerminal(macStream,cache)
//  }
//
//  def resolveMacTerminal(macSteam : DStream[TerminalInfo],cache:mutable.HashMap[String, RDD[TerminalInfo]]): Unit ={
//    val filteredTerminalData = macSteam.map(x => {
//      (x.getDevNo+"_"+x.getMACAddress,x)
//    })
//    filteredTerminalData.foreachRDD(x => x)
//    val distinctTerminalData = Start.reduceByKeyAndWindowTerminalInfo(filteredTerminalData,cache)
//    //distinctTerminalData.foreachRDD()
//  }
//  def reduceByKeyAndWindowTerminalInfo(filteredTerminalData:DStream[(String, TerminalInfo)],cache:mutable.HashMap[String, RDD[TerminalInfo]]): DStream[(String, TerminalInfo)] ={
//    filteredTerminalData.reduceByKeyAndWindow((x:TerminalInfo,y:TerminalInfo) => {
//      y
//    },Seconds(5),Seconds(5))
//  }
//}
