package kafkautili

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util
import java.util.concurrent.{Callable, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

case class Producer(
    bootStrapServer: String,
    acks: String = "-1",
    threadCount: Int = 5,
    batchSize: Int = 10240
) {
  val configs: Map[String, Object] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServer,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.ACKS_CONFIG -> acks,
    ProducerConfig.BATCH_SIZE_CONFIG -> batchSize.toString
  )

  private val sender =
    new KafkaProducer[String, String](JavaConverters.mapAsJavaMap(configs))

  def SendMessage(
      topic: String,
      key: String,
      value: String,
      partition: Integer,
      callback: Callback
  ): RecordMetadata = {
    val data = new ProducerRecord[String, String](topic, partition, key, value)
    sender.send(data, callback).get
  }

  def SendMessage(
      topic: String,
      datas: Map[String, String],
      partition: Integer = null,
      callback: Callback = null,
      errorHandler: Exception => Unit = null
  ): Array[RecordMetadata] = {

    val service = new ThreadPoolExecutor(
      threadCount,
      threadCount + 5,
      1000L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable]
    )
    val tasks = new util.ArrayList[Callable[RecordMetadata]]
    datas.foreach(t => {
      tasks.add(() => SendMessage(topic, t._1, t._2, partition, callback))
    })

    try {
      val invokeResult = service.invokeAll(tasks)
      val returnResult = new ArrayBuffer[RecordMetadata]
      invokeResult.forEach(t => { returnResult.append(t.get) })
      returnResult.toArray
    } catch {
      case ex: Exception =>
        if (errorHandler != null) {
          errorHandler(ex)
        }
        null
    } finally {
      service.shutdown()
    }
  }
}
