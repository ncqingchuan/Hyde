package kafkautili

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import java.util.Collection
import org.apache.kafka.clients.consumer.ConsumerRecord

final case class Consumer(
    bootStrapServer: String,
    groupID: String,
    autoCommit: Boolean = false
) {

  val configs: Map[String, Object] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServer,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[
      StringDeserializer
    ],
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> autoCommit.toString,
    ConsumerConfig.GROUP_ID_CONFIG -> groupID

  )

  def RecieveMessage(
      topic: String,
      partition: Integer,
      duration: Long = 1000L,
      messageHandler: ConsumerRecord[String, String] => Unit,
      errorHandler: Exception => Unit = null
  ): Unit = {
    val consumer: KafkaConsumer[String, String] =
      new KafkaConsumer(JavaConverters.mapAsJavaMap(configs))
    val topics = topic :: Nil
    val topicPartitions = new TopicPartition(topic, partition) :: Nil

    if (partition == null) {
      consumer.subscribe(
        JavaConverters.seqAsJavaList(topics),
        new MyRebalanceListener(consumer)
      )
    } else {
      consumer.assign(JavaConverters.seqAsJavaList(topicPartitions))
    }

    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(duration))
        records.forEach(t => {
          messageHandler(t)
        })
        if (!autoCommit) {
          consumer.commitSync()
        }
      }
    } catch {
      case e: Exception =>
        if (errorHandler != null) {
          errorHandler(e)
        }
    } finally {
      consumer.close()
    }
  }

  class MyRebalanceListener(consumer: KafkaConsumer[String, String])
      extends ConsumerRebalanceListener {
    def onPartitionsAssigned(partitions: Collection[TopicPartition]): Unit = {
      var position = -1L
      partitions.forEach(t => {
        position = consumer.position(t)
        consumer.seek(t, position + 1)
      })
    }
    def onPartitionsRevoked(partitions: Collection[TopicPartition]): Unit = {
      consumer.commitSync()
    }
  }
}
