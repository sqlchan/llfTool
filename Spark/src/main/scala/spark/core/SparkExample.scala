package spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val line = sc.textFile("D:\\code\\llf\\llfTool\\Spark\\src\\main\\resources\\a.txt")
    val pairs: RDD[(String, Int)] = line.flatMap(_.split(" ")).map(x => (x, 1))

    // AggregateByKey
    //val wordCount = pairs.reduceByKey(_+_)
    // aggregateByKey，分为三个参数
    // reduceByKey认为是aggregateByKey的简化版
    // aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
    // 就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
    // 然后才是对所有partition中的数据进行全局聚合

    // 第一个参数是，每个key的初始值
    // 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
    // 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合
    val wordCount = pairs.aggregateByKey(0)((v1: Int, v2: Int) => (v1 + v2), (v1: Int, v2: Int) => (v1 + v2))
    wordCount.collect().foreach(println)


    val wordCount1 = pairs.reduceByKey(_+_)
    val wordSort1 = wordCount1.map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1))
    wordSort1.collect()

    // CombineByKey
    // createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
    // mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
    // mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
    val data = Array((1,1.0),(1,2.0),(1,3.0),(2,4.0))
    val rdd = sc.parallelize(data,2)
    val combine1 = rdd.combineByKey(
      createCombiner = (v:Double) => (v:Double, 1),
      mergeValue =  (c:(Double, Int),v:Double) => (c._1 +v, c._2 +1),
      mergeCombiners = (c1:(Double, Int), c2:(Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2
    )

    // MapPartitionWithIndex
    val studentNames = List("张三", "李四", "王五", "赵六")
    val studentNamesRDD = sc.parallelize(studentNames, 2)

    // mapPartitionWithIndex可以拿到每个partition的index
    val studentNamesWithClass = studentNamesRDD.mapPartitionsWithIndex(
      (index: Int, iterator: Iterator[String]) => Iterator {
        //  val studentWithClassList: ArrayBuffer[String] = new ArrayBuffer[String]
        val studentWithClassList = new util.ArrayList[String]()
        while(iterator.hasNext) {
          val studentName = iterator.next()
          val studentWithClass = studentName + "_" + (index + 1)
          //                    studentWithClass +=: studentWithClassList
          studentWithClassList.add(studentWithClass)
        }
        studentWithClassList
      }, true);

    studentNamesWithClass.foreach(println)

    // Sample
    val staffList = List("张三", "李四", "王五", "赵六", "张三_1", "李四_1", "王五_1", "赵六_1", "王五_2", "赵六_2")
    val staffRDD = sc.parallelize(staffList, 2)
    // sample算子，随机抽取
    val luckBoy = staffRDD.sample(false, 0.1)
    luckBoy.foreach(println)



  }
}
