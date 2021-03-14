package ods

import java.{lang, util}

import Utils.KafkaUtil
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.immutable.Map

object BaseDbCanal {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("BaseDbCanal").setMaster("local")
    val context: StreamingContext = new StreamingContext(conf,Seconds(4))

    val topics: Array[String] = Array("gmall_db_canal")
    val groupId = "program-group"
    val kafkaParam: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
    "group.id" -> groupId,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false")


    //查询redis中是否有偏移量信息 若有就从偏移量创建stream
    val jedis: Jedis = getJedis()
    val offsetMap: util.Map[String, String] = jedis.hgetAll(topics(0) + groupId)
    jedis.close()

    offsetMap.entrySet().forEach(x => {
      println(x.getKey + " : " + x.getValue)
    })


    var newMap: Map[TopicPartition, Long] = Map.empty[TopicPartition, Long]
    offsetMap.entrySet().forEach(x => {
      val newKey: TopicPartition = new TopicPartition(topics(0), x.getKey.toInt)
      val newValue: Long = x.getValue.toLong
      newMap += (newKey -> newValue)
    })

    var dStream: InputDStream[ConsumerRecord[String, String]] = null
    if (newMap.isEmpty){
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    }else{
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics,kafkaParam,newMap))
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
        //maxwell和canal的区别就在这里 canal将一条sql影响的多行数据放在一个数组中 maxwell将一条sql影响的数据分开存放
        val array: JSONArray = jsonObj.getJSONArray("data")
        val tableName: String = jsonObj.getString("table")

        //分流操作 将不同数据分发到不同的topic
        val topic = "ODS_" + tableName.toUpperCase()
        array.forEach(x => KafkaUtil.kafkaSink(topic,null,x.toString))
      })

      //提交偏移量
      val jedis: Jedis = getJedis()
      for (elem <- ranges) {
        val topic: String = elem.topic
        val partition: Int = elem.partition
        val offset: Long = elem.untilOffset

        val key = topic + groupId
        jedis.hset(key,partition.toString,offset.toString)
      }
      jedis.close()
    })

    context.start()
    context.awaitTermination()
  }

  def getJedis(): Jedis ={
    val pool: JedisPool = new JedisPool("hadoop02")
    pool.getResource
  }

}
