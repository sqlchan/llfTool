package spark.core
import java.util

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("new").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text = sc.textFile("D:\\code\\llf\\llfTool\\Spark\\src\\main\\resources\\a.txt");
    val pairs = text.flatMap(_.split(" ")).map(x => (x,1))
    //val count = pairs.reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(true).map(x => (x._2,x._1))
    //val count = pairs.aggregateByKey(0)((v1:Int,v2:Int)=>(v1+v2),(v1:Int,v2:Int)=>(v1+v2))
    val count = pairs.combineByKey(
      (v: Int) => v,
      (v1:Int,v2:Int)=>(v1+v2),
      (v1:Int,v2:Int)=>(v1+v2),
      2
    )
    count.collect().foreach(println) // (d,1) (b,1)
    val mapwithpattern = pairs.mapPartitionsWithIndex(
      (index : Int,iterator:Iterator[(String,Int)])=>Iterator{
        val studentWithClassList = new util.ArrayList[String]()
        while(iterator.hasNext) {
          val studentName = iterator.next()
          val studentWithClass = studentName + "_" + (index + 1)
          //                    studentWithClass +=: studentWithClassList
          studentWithClassList.add(studentWithClass)
        }
        studentWithClassList
      },true
    )
    //mapwithpattern.collect().foreach(println)

    pairs.sample(false,0.5).collect().foreach(println)
    val data = Array((1,1.0),(1,2.0),(1,3.0),(2,4.0))
    val rdd = sc.parallelize(data,2)
    val combine1 = rdd.combineByKey(
      createCombiner = (v:Double) => (v:Double, 1),
      mergeValue =  (c:(Double, Int),v:Double) => (c._1 +v, c._2 +1),
      mergeCombiners = (c1:(Double, Int), c2:(Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2
    )
    combine1.collect().foreach(println) // (2,(4.0,1)) (1,(6.0,3))
  }
}

