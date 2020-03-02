package spark.kafka

object KafkaParam {
  val kafkaParam:Map[String,String] = Map[String,String]("group.id" -> "llfmac",
  "zookeeper.connect" -> "10.33.57.46:2181")
  val directkafkaParam:Map[String,String] = Map[String,String]("group.id" -> "llfmac",
    "bootstrap.servers" -> "10.33.57.46:9092"
  )
}
