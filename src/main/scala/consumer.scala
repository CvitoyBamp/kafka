import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties
import scala.collection.JavaConverters._
import java.util
import java.time.Duration

object consumer extends App{

  case class Bestsellers(name: String, author: String, userRating: Double, reviews: Int , price: Int , year: Int , genre: String)

  val connectionProperties = new Properties()
    connectionProperties.put("bootstrap.servers", "localhost:9092")
    connectionProperties.put("group.id", "consumer1")

  val kafkaConsumer = new KafkaConsumer(connectionProperties, new StringDeserializer, new StringDeserializer)

  val topicName = "books"

  val MaxPartition = 5

  kafkaConsumer.subscribe(List(topicName).asJavaCollection)

  val partitions = kafkaConsumer.partitionsFor(topicName).asScala
  val topicList = new util.ArrayList[TopicPartition]()

  partitions.foreach {
    xs =>
      topicList.add(new TopicPartition(xs.topic(), xs.partition()))
  }

  val messages = kafkaConsumer.poll(Duration.ofSeconds(1))
  kafkaConsumer.seekToEnd(topicList)

  val partitionOffset = new util.HashMap[String, Long]()
  for (partition <- topicList.asScala) {
    partitionOffset.put(partition.toString, kafkaConsumer.position(partition) - 1)
  }

  val buffer = new util.HashMap[String, List[String]]()
  for (msg <- messages.asScala) {
    val key = s"${msg.topic()}-${msg.partition()}"

    if (msg.offset() >= partitionOffset.get(key) - MaxPartition && msg.offset() < partitionOffset.get(key)) {
      val msgWithOffset = s"offset: ${msg.offset()} | msg: ${msg.value()}"
      if (!buffer.containsKey(key)) {
        buffer.put(key, List(msgWithOffset))
      } else {
        val currentMsgList = buffer.get(key)
        if (currentMsgList.length <= MaxPartition){
          val newMsgList = msgWithOffset :: currentMsgList
          buffer.put(key, newMsgList)
        }
      }
    }
  }

  for (partition <- topicList.asScala) {
    val key = partition.toString
    println(s"Topic: $key")
    buffer.getOrDefault(key, List()).foreach(println)
  }

}