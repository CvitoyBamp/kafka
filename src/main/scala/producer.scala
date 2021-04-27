import org.apache.commons.csv.CSVFormat
import java.io.FileReader
import io.circe.Encoder
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object producer extends App{

  case class Bestsellers(name: String, author: String, userRating: Double, reviews: Int , price: Int , year: Int , genre: String)

  val in = new FileReader("D:\\scala\\kafkahw\\data\\bestsellers_with_categories-1801-9dc31f.csv")

  val recordsHead = CSVFormat.EXCEL.withHeader().parse(in).getHeaderNames

  implicit val encodeFieldType: Encoder[Bestsellers] =
    Encoder.forProduct7(recordsHead.get(0), recordsHead.get(1),recordsHead.get(2),recordsHead.get(3),recordsHead.get(4),recordsHead.get(5),recordsHead.get(6))(Bestsellers.unapply(_).get)

  val recordsData  = CSVFormat.EXCEL.withHeader().parse(in).getRecords

  val connectionProperties = new Properties()
  connectionProperties.put("bootstrap.servers", "localhost:9092")

  val kafkaProducer = new KafkaProducer(connectionProperties, new StringSerializer, new StringSerializer)

  val topicName = "books"

  val result: Seq[Bestsellers] = (for(i <- 1 until recordsData.size()) yield Bestsellers(recordsData.get(i).get(0),recordsData.get(i).get(1),recordsData.get(i).get(2).toDouble,recordsData.get(i).get(3).toInt,recordsData.get(i).get(4).toInt,recordsData.get(i).get(5).toInt,recordsData.get(i).get(6))).toList

  val resultJson = result.asJson.toString

  kafkaProducer.send(new ProducerRecord(topicName, resultJson, resultJson))

}

