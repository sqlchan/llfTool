//package spark.kafka
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import spark.getMac.FormatMAC
//
//object KafkaToKafka {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc,Seconds(5))
//    //val macStream = KafkaUtils.createStream(ssc,"10.33.57.46:2181","llfmac1",Map("MAC_INFO_TOPIC" -> 1)).map(_._2)
//    //val macStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,KafkaParam.kafkaParam,Map("MAC_INFO_TOPIC" -> 1),StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
//    val macStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,KafkaParam.directkafkaParam,Set("MAC_INFO_TOPIC"))
////    macStream.foreachRDD(x =>{
////      println(x.toString())
////      x.collect().foreach(println)
////    })
//// hold a reference to the current offset ranges, so it can be used downstream
//    var offsetRanges = Array[OffsetRange]()
//    val tf = macStream.transform { rdd =>
//      // Get the offset ranges in the RDD
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      //for (x <- offsetRanges)(println(x))
//      offsetRanges.foreach(println)
//      rdd
//    }
//
//    tf.foreachRDD { rdd =>
//      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
//        // For each partition, get size of the range in the partition,
//        // and the number of items in the partition
//        // 对于每个分区，获取分区中范围的大小以及分区中的项目数
//        val off = offsetRanges(i)
//        val all = iter.toSeq
//        val partSize = all.size
//        val rangeSize = off.untilOffset - off.fromOffset
//        Iterator((partSize, rangeSize))
//      }.collect
//
//      // Verify whether number of elements in each partition
//      // matches with the corresponding offset range
//      collected.foreach { case (partSize, rangeSize) =>
//      }
//    }
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
