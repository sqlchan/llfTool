package spark.streaming

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Seconds

import scala.collection.mutable.ArrayBuffer

object StreamingExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    // WordCount
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map((_, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false) // false表示不关闭SparkContext


    //WindowHotWord 热点搜索词滑动统计，每隔10秒，统计最近60秒钟的搜索词的搜索频次
    val searchLogsDStream = ssc.socketTextStream("localhost", 9999)
    // 将搜索日志转换为只有一个搜索词，即可
    val searchWordDStream = searchLogsDStream.map(searchLog => searchLog.split(" ")(1))
    val searchWordPairDStream = searchWordDStream.map((_, 1))
    // (searchWord, 1)
    val searchWordCountsDStream = searchWordPairDStream.reduceByKeyAndWindow(_+_, _-_, Durations.seconds(60), Durations.seconds(10))
    // 执行transform操作,一个窗口就是一个60秒的数据，会变成一个RDD，然后，对这个RDD根据每个搜索词出现的频率进行排序
    val finalDStream = searchWordCountsDStream.transform {searchWordCountsRDD => {
      val countSearchWordRDD = searchWordCountsRDD.map(x => (x._2, x._1))
      val sortedCountSearchWordsRDD = countSearchWordRDD.sortByKey(false)
      val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._2, tuple._1))
      val top3SearchWordCounts = sortedSearchWordCountsRDD.take(3)
      for (tuple <- top3SearchWordCounts) {
        println(tuple)
      }
      searchWordCountsRDD
    }}
    finalDStream.print()
    ssc.start()
    ssc.awaitTermination()


    // UpdateStateByKey
    // 必须设置checkpoint
    val checkpointDir = "./data/checkpoint/"
    ssc.checkpoint(checkpointDir)
    val words1 = lines.flatMap(_.split(" "))
    val pairs1 = words1.map((_, 1))
    // 统计全局的单词计数
    // 使用updateStateByKey可以维护一份全局的

    def updateState(values: Seq[Int], state: Option[Int]) = {
      var newValues = state.getOrElse(0)
      for (value <- values) {
        newValues += value
      }
      Some(newValues)
    }
    val wordCounts1 = pairs1.updateStateByKey(updateState)
    wordCounts1.print()

    //TransformBlacklist
    val blacklist = ArrayBuffer[(String, Boolean)](
      ("zhangsan", true),
      ("lisi", true))
    val blacklistRDD = ssc.sparkContext.parallelize(blacklist)
    val adsClickLogDStream = ssc.socketTextStream("localhost", 9999)
    // (username, date username)
    val userAdsClickLogDStream = adsClickLogDStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))
    val validAdsClickLogDStream = userAdsClickLogDStream.transform {
      userAdsClickLogRDD => {
        val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
        val filteredRDD = joinedRDD.filter(!_._2._2.getOrElse(false))
        val validAdsClickLogRDD = filteredRDD.map(_._2._1)
        validAdsClickLogRDD
      }
    }
    validAdsClickLogDStream.print()


  }
}
