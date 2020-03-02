package com.llf.spark

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkExampleMain").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("D:\\code\\llf\\llfTool\\SparkTest\\src\\main\\resources\\a.txt")
    val pairs = text.flatMap(_.split(" ")).map(x => (x,1))
    val words = pairs.reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey().map(x => (x._2,x._1))
    words.foreach(println)

  }
}
