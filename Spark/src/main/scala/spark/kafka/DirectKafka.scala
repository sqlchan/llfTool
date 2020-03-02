package spark.kafka

import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

object DirectKafka {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("haha")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val preferredHosts = LocationStrategies.PreferConsistent

    val topic = "MAC_INFO_TOPIC"
    val kafkaParams: collection.Map[String, Object] = Map("group.id" -> "llfmac",
      "bootstrap.servers" -> "10.33.57.46:9092",
      "enable.auto.commit" -> "false",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer])

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams))

    kafkaStream.foreachRDD { (rdd: RDD[ConsumerRecord[String, String]], time: Time) =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsets.foreach(x => println(x))
      val data = rdd.map(_.value).collect()
      data.foreach(x => println(x))
      println("============")
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)  //手动提交offset,更改命令和topic中的offset，zk中的值未更改
      persistOffsets(offsets,"llfmac",false)  //仅更改zk中的值，命令和topic中的offset未更改
      //var map: Map[TopicPartition, Long] = readOffsets(Seq("MAC_INFO_TOPIC"),"llfmac")

//      kafkaStream.asInstanceOf[CanCommitOffsets]
//        .commitAsync(offsets, new OffsetCommitCallback() {
//          def onComplete(m: JMap[TopicPartition, OffsetAndMetadata], e: Exception) {
//            if (null != e) {
//
//            } else {
//              committed.putAll(m)
//            }
//          }
//        })
    }

    ssc.start()
    ssc.awaitTermination()
  }

  val zkClientAndConnection = ZkUtils.createZkClientAndConnection("10.33.57.46:2181", 1200, 1200)
  val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
  def readOffsets(topics: Seq[String], groupId:String): Map[TopicPartition, Long] = {
    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)
    // /consumers/<groupId>/offsets/<topic>/
    partitionMap.foreach(topicPartitions =>{
      val zkGroupTipicDirs = new ZKGroupTopicDirs(groupId,topicPartitions._1)
      topicPartitions._2.foreach(partition =>{
        val offsetPath = zkGroupTipicDirs.consumerOffsetDir + "/" + partition
        try{
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (null != offsetStatTuple){
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1,Integer.valueOf(partition)),offsetStatTuple._1.toLong)
          }
        }catch {
          case e:Exception =>
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1,Integer.valueOf(partition)),0L)
        }
      })
    })
    topicPartOffsetMap.toMap
  }

  //更改zk中的值
  def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean): Unit = {
    offsets.foreach(or =>{
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      val acls = new ListBuffer[ACL]()
      val acl = new ACL
      acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
      acl.setPerms(ZooDefs.Perms.ALL)
      acls += acl
      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))
    })
  }
}
