package disaster

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

object TestPrint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TestPrint").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(2))

    val topics: Array[String] = Array("t1")
    val kafkaParam: Map[String, Object] = Map[String, Object]("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "group.id" -> "g1",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false")


    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))


    dStream.transform(rdd => {
      val value: RDD[String] = rdd.map(x => x.value())
      value
    }).print(100)

    context.start()
    context.awaitTermination()
  }
}
