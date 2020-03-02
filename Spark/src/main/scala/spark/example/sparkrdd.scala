package spark.example

import org.apache.spark.{SparkConf, SparkContext}

object sparkrdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd1to5 = sc.makeRDD(1 to 5,2)
    val rdd6to10 = sc.makeRDD(6 to 10,2)
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    var list = List(1,2,3,4)
    val listrdd = sc.parallelize(list,2)
    val listarr = sc.parallelize(arr, 3)
    val listarr1 = sc.parallelize(arr1, 3)
    listrdd.cache()

    listrdd.map(x => x*10).foreach(print)
    listrdd.flatMap(x => 1 to x).foreach(print)
    listrdd.filter(x => x>2).foreach(print)
    listrdd.distinct().foreach(print)
    println(listrdd.getNumPartitions)
    println(listrdd.repartition(3).getNumPartitions)
    println(listrdd.coalesce(2).getNumPartitions)
    listrdd.sample(false,0.4,3).foreach(print)
    listrdd.randomSplit(Array(0.3,0.7),3).foreach(x => print(x.foreach(print)))
    listrdd.takeSample(false,2,3).foreach(print)
    rdd1to5.union(rdd6to10).foreach(print)
    listarr.sortBy(x =>x._1).foreach(print)
    listrdd.glom().foreach(glomdef)
    // groupBy算子接收一个函数，这个函数返回的值作为key，然后通过这个key来对里面的元素进行分组
    println(listrdd.groupBy(x => {if (x % 2 ==0) "even" else  "odd"}).collect().toString)
    listrdd.mapPartitions(mappart).foreach(print)
    listrdd.mapPartitionsWithIndex(mappartwithindex).foreach(print) //每个partition的index
  }
  def glomdef(array:Array[Int]): Unit ={
    println(array.length)
  }
  def mappart(iterator: Iterator[Int]): Iterator[Int] ={
    var list = List[Int]()
    while (iterator.hasNext){
      var x = iterator.next()
      list = x :: list
    }
    list.iterator
  }
  def mappartwithindex(i:Int,iterator: Iterator[Int]): Iterator[Int] ={
    var list = List[Int]()
    while (iterator.hasNext){
      var x = iterator.next()
      list = x :: 9::i :: list
    }

    list.iterator
  }

}
