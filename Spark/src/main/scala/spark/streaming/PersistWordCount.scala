package spark.streaming

import java.sql.Statement

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.jetty.client.ConnectionPool


/**
  * 改写UpdateStateByKeyWordCount，将每次统计出来的全局的单词计数，写入一份
  *
  * Created by yangtong on 17/6/19.
  */
object PersistWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    // 必须设置checkpoint
    val checkpointDir = "./data/checkpoint/"
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map((_, 1))

    // 统计全局的单词计数
    // 使用updateStateByKey可以维护一份全局的

    def updateState(values: Seq[Int], state: Option[Int]) = {
      var newValues = state.getOrElse(0)
      for (value <- values) {
        newValues += value
      }
      Some(newValues)
    }

    val wordCounts = pairs.updateStateByKey(updateState)

    //        wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

