package spark.example

import org.apache.spark.{SparkConf, SparkContext}

object sparkrddaction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd1to5 = sc.makeRDD(1 to 5,2)
    val rdd6to10 = sc.makeRDD(6 to 10,2)
    val arr = Array(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    var list = List(1,2,3,4)
    val listrdd = sc.parallelize(list,2)
    val listarr = sc.parallelize(arr, 3)
    val listarr1 = sc.parallelize(arr1, 3)

    listrdd.foreachPartition(foreachpp)
    listrdd.collect().foreach(print)
    println(listrdd.reduce(_+_))
    println(listrdd.fold(1)(_+_))
    println(listrdd.aggregate(1)((_+_),(_+_)))
    println(listrdd.count())
    listrdd.zipWithIndex()
    listarr.combineByKey(
      (v:Int) => (v,1),
      (c:(Int,Int),v1:Int) =>(c._1+v1,c._2+1),
      (c1:(Int,Int),c2:(Int,Int)) =>(c1._1+c2._1,c1._2+c2._2),2).collect().foreach(print)

  }

  def foreachpp(i:Iterator[Int]): Unit ={

  }
}
