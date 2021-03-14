package ods

import java.util

import Utils.{KafkaUtil, OffsetToHdfs, RedisUtil}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.immutable.Map

object BaseDbMaxwell {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("BaseDbMaxwell").setMaster("local")
    val context: StreamingContext = new StreamingContext(conf,Seconds(4))

    val topics: Array[String] = Array("GMALL_DB_MAXWELL")
    val groupId = "MAXWELL-G1"
    val kafkaParam: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
    "group.id" -> groupId,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false")

    val offsetMap: collection.Map[TopicPartition, Long] = OffsetToHdfs.getOffset(topics(0), groupId)
    var dStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap == null || offsetMap.size == 0){
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    }else{
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam, offsetMap))
    }

    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offSetStream: DStream[ConsumerRecord[String, String]] = dStream.transform(rdd => {
      //获取偏移量
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    offSetStream.foreachRDD(rdd => {
      rdd.foreach(record => {
        val jsonString: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonString)
        val strType: String = jsonObj.getString("type")
        if (!strType.equals("bootstrap-start") && !strType.equals("bootstrap-complete")) {
          //maxwell和canal的区别就在这里 canal将一条sql影响的多行数据放在一个数组中 maxwell将一条sql影响的数据分开存放
          val dataJson: String = jsonObj.getString("data")
          val tableName: String = jsonObj.getString("table")

          if (dataJson != null && !dataJson.isEmpty && !tableName.equals("bootstrap")) {
            //分流操作 将不同数据分发到不同的topic
            val topic = "ODS_" + tableName.toUpperCase()
            KafkaUtil.kafkaSink(topic,null, dataJson)

            if (tableName.equals("user_info")) {
              println(dataJson)
            }
          }
        }
      })

      //提交偏移量
      OffsetToHdfs.saveOffset(ranges, topics(0), groupId)
    })

    context.start()
    context.awaitTermination()
  }

}
