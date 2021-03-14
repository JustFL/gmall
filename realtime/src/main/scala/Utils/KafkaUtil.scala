package Utils

import java.time.Duration
import java.{io, util}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * printf 格式化输出
 * %d 十进制数字
 * %s 字符串
 * %c 字符
 * %e 指数浮点数
 * %f 浮点数
 * %i 整数（十进制）
 * %o 八进制
 * %u 无符号十进制
 * %x 十六进制
 * %% 打印%
 * % 打印%
 */

object KafkaUtil {
  def main(args: Array[String]): Unit = {

  }


  def KafkaConsumeData(topic : String): Unit = {

    val kafkaMap: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "earliest",
      "group.id" -> "start-g1")
    import scala.collection.JavaConverters._
    val map: util.Map[String, Object] = kafkaMap.asJava
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](map)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val consumers: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(2))
      val iter: util.Iterator[ConsumerRecord[String, String]] = consumers.iterator()
      while (iter.hasNext){
        val record: ConsumerRecord[String, String] = iter.next()
        printf("当前数据key为%s,value为%s,分区号为%d,分区内offset为%d\n",record.key(),record.value(),record.partition(),record.offset())
      }
    }
  }

  def kafkaSink(topic : String, key : String, value: String): Unit = {
    val map: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer])
    import scala.collection.JavaConverters._
    val mapAsJava: util.Map[String, Object] = map.asJava
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](mapAsJava)
    producer.send(new ProducerRecord[String,String](topic, key, value))
    producer.close()
  }
}
