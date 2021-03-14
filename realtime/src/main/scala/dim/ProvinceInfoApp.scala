package dim

import bean.ProvinceInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.phoenix.spark._

object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ProviceInfoApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topics: Array[String] = Array("ODS_BASE_PROVINCE")
    val groupId = "ODS_BASE_PROVINCE_G0"
    val kafkaParam: Map[String, Object] = Map[String, Object]("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "group.id" -> groupId)
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    val infoStream: DStream[ProvinceInfo] =
      dStream.map(x => {
      val value: String = x.value()
      val info: ProvinceInfo = JSON.parseObject(value, classOf[ProvinceInfo])
      info
    })

    //id, name, region_id, area_code, iso_code, iso_code_3166_2
    infoStream.foreachRDD(rdd =>
      rdd.saveToPhoenix("PROVINCE_INFO",
        Seq("ID", "NAME", "REGION_ID", "AREA_CODE", "ISO_CODE", "ISO_CODE_3166_2"),
        new Configuration,
        Some("hadoop01,hadoop02:2181")))

    //maxwell引导命令
    //maxwell-bootstrap --user maxwell --password maxwell --host hadoop01 --database gmall --table base_province --client_id maxwell1

    context.start()
    context.awaitTermination()
  }
}
