package spark.example

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object kvRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    val rdd1to5 = sc.makeRDD(1 to 5,2)
    val rdd6to10 = sc.makeRDD(6 to 10,2)
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val listrdd = sc.parallelize(arr, 3)
    val listrdd1 = sc.parallelize(arr1, 3)
    println(rdd1to5.getNumPartitions.toString)
    val r = ssc.sparkContext.parallelize(arr,2)
    // aggregate函数将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
    // 这个函数最终返回的类型不需要和RDD中元素类型一致。
    // def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    // 每个分区开始聚合第一个元素都是zeroValue
    // 分区之间的聚合，zeroValue也参与运算
    // 先对分区内的元素进行聚合
    // 对聚合后的分区之间进行聚合
    val aggreaterdd = rdd1to5.aggregate(1)((x,y)=> x+y,(a,b)=> a+b)
    println("aggreate")
    println(aggreaterdd)
    // reduce()与fold()方法是对同种元素类型数据的RDD进行操作，即必须同构。其返回值返回一个同样类型的新元素。
    // fold()与reduce()类似，接收与reduce接收的函数签名相同的函数，另外再加上一个初始值作为第一次调用的结果。
    val foldrdd = rdd1to5.fold(0)((x,y)=> x+y)
    val reducerdd = rdd1to5.reduce((x,y)=> x+y)
    //println(foldrdd)
    val cogrouprdd = listrdd.cogroup(listrdd1)
    cogrouprdd.foreach(println)
    println(cogrouprdd.toDebugString)

    val groupbykeyrdd = listrdd.groupByKey()
    groupbykeyrdd.foreach(println)
    val reducebykeyrdd = listrdd.reduceByKey(_+_)
    reducebykeyrdd.foreach(println)
    val comebinbykeyrdd = listrdd.combineByKey(
      createCombiner = (v:Int) => (v:Int, 1),
      mergeValue =  (c:(Int, Int),v:Int) => (c._1 +v, c._2 +1),
      mergeCombiners = (c1:(Int, Int), c2:(Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2
    )
    comebinbykeyrdd.foreach(println)  // (A,(3,2))  (B,(5,2))

    val faltmapvaluesrdd = listrdd.flatMapValues(x => Seq(x,"aaa"))
    faltmapvaluesrdd.foreach(println)

    // foldByKey操作作用于RDD[K,V]根据K将V做折叠、合并处理，其中的参数zeroValue表示先根据映射函数将zeroValue应用与V，进行初始化V，在将映射函数应用于初始化后的V。
    val foldbykeyrdd = listrdd.foldByKey(2)(_+_)
    foldbykeyrdd.foreach(println)




  }
}
