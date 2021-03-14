package Utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object DStreamUtil {

  def createDStream(topic: String, groupid: String, context: StreamingContext, offset: collection.Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    val topics = Array(topic)
    val kafkaParam: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "group.id" -> groupid,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest")

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam, offset))
    dStream
  }

  def createDStream(topic: String, groupid: String, context: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val topics = Array(topic)
    val kafkaParam: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "group.id" -> groupid,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest")

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    dStream
  }

}