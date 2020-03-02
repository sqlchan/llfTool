package spark.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object transForm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val collect = Seq((1 to 10,Seq("aa","bb")),(11 to 15,Seq("b","a")))
    var collectRdd = sc.makeRDD(collect)

    val rdd1to5 = sc.parallelize( 1 to 5,2)
    val rdd6to10 = sc.parallelize( 6 to 10,2)
    val map = rdd1to5.map(_*2)
    map.foreach(x => print(x+"  ")) // 2  6  8  4  10  随机
    map.collect().foreach(x => print(x+"  "))  //2  4  6  8  10

    val flatmap = rdd1to5.flatMap(x => (1 to x))
    flatmap.foreach(x => print(x+"  "))  // 1  1  2  1  3  2  1  2  3  4  1  2  3  4  5

    val list = List(("a",1),("b",2),("a",3),("c",4))
    var rddlist = sc.parallelize(list,2)
    val mappartitions = rddlist.mapPartitions(partitionFun)
    mappartitions.foreach(x => print(x+"  "))  // (b,2)_  (a,1)_  (c,4)_  (a,3)_

    val samplerdd = rdd1to5.sample(true,0.5,3)
    samplerdd.foreach(x => print(x+"  "))

    val unionrdd = rdd1to5.union(rdd6to10)
    unionrdd.collect().foreach(x => print(x+"un  "))

    val dislist = List(1,1,2,3,4,4,3)
    val dislistrdd = sc.parallelize(dislist)
    val disrdd = dislistrdd.distinct()
    disrdd.collect().foreach(x => print(x+"un  "))

    println(rddlist.partitions.size)
    var rddlist1 = rddlist.coalesce(4,true)
    // 对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，不能增加分区数目,但不会报错，只是分区个数还是原来的
    println(rddlist.partitions.size) // 对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，不能增加分区数
    println(rddlist1.partitions.size)

    val glomRDD = rdd1to5.glom()
    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))

    val randomSplitRDD = rdd6to10.randomSplit(Array(3.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +"_00"))
    println()
    randomSplitRDD(1).foreach(x => print(x +"_11"))

    val cartesianRDD = rdd1to5.cartesian(rdd6to10)
    cartesianRDD.foreach(x => println(x+" __"))

    val rdd1 = sc.makeRDD(Seq("a","b","c"),2)
    rdd1.zipWithIndex().foreach(println)
    rdd1.zipWithUniqueId().foreach(println)

  }

  def partitionFun(iter: Iterator[(String,Int)] ): Iterator[String] ={
    var list = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      list = next+"_" :: list
    }
    list.iterator
  }
}
