package disaster

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaBaseConsumer {
  def main(args: Array[String]): Unit = {

    val kafkaParam: util.HashMap[String, Object] = new util.HashMap[String, Object]()
    kafkaParam.put("bootstrap.servers","hadoop01:9092,hadoop02:9092")
    kafkaParam.put("key.deserializer",classOf[StringDeserializer])
    kafkaParam.put("value.deserializer",classOf[StringDeserializer])
    kafkaParam.put("group.id","T1_G1")
    //[latest, earliest, none]
    kafkaParam.put("auto.offset.reset","latest")
    kafkaParam.put("enable.auto.commit",(false:java.lang.Boolean))

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaParam)
    val topics: util.List[String] = util.Arrays.asList("T1")
    consumer.subscribe(topics)

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(4000))
      val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
      while (iter.hasNext) {
        val record: ConsumerRecord[String, String] = iter.next()
        printf("分区:%d 偏移量%d value:%s\n",record.partition(),record.offset(),record.value())
      }
    }

    consumer.close()
  }
}
