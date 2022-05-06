import kafkautili.Producer
import scala.collection.mutable
import org.apache.kafka.clients.producer.RecordMetadata
import kafkautili.Consumer
object App {
  def main(args: Array[String]): Unit = {
    // println("hello world")
    val server = "bigdata:9092"
    val topic = "kafka-action"
    Consumer(server, "test").RecieveMessage(
      topic,
      partition = null,
      messageHandler = t => {
        println(t.value())
      }
    )
  }
}
