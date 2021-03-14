package ods

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import Utils.{EsUtil, OffsetToRedis, RedisUtil}
import bean.DauInfo
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DaliyDataHandle {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DaliyDataHandle")
    val context: StreamingContext = new StreamingContext(conf, Seconds(4))

    val groupId = "GMALL_LOG_START_G0"
    val topicName = "GMALL_LOG_START"
    val kafkaParam: Map[String, Object] = Map(
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "group.id" -> groupId,
      //Valid Values:	[latest, earliest, none]
      "auto.offset.reset" -> "earliest",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics: Array[String] = Array(topicName)

    //每次先去redis读取偏移量
    val offset: Map[TopicPartition, Long] = OffsetToRedis.getOffSet(topicName, groupId)
    var dStream: InputDStream[ConsumerRecord[String, String]] = null

    if (offset == null || offset.size == 0) {
      //没有偏移量从头读取
      dStream = KafkaUtils.createDirectStream[String, String](context, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    } else {
      //将偏移量传入 从偏移量以后开始读取
      dStream = KafkaUtils.createDirectStream[String, String](context, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParam, offset))
    }

    /**
     * 当dStream创建完成就代表kafka中的数据已经被消费了 后续所有的转化都只是自己的业务 这dStream中的那几个rdd中的数据已经算消费掉了 所以这里就可以获取本次批量的偏移量了
     */
    //获取当前 InputDStream[ConsumerRecord[String, String]] 中每个partition中的偏移量存放在 Array[OffsetRange] 中
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    //transform和map的区别 map是针对rdd中的每一个元素的 transform是针对一串rdd中的每一个rdd的
    val offSetStream: DStream[ConsumerRecord[String, String]] = dStream.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //这句执行在dirver中
//      for (elem <- ranges) {
//        println("partition：" + elem.partition + " --- " + elem.fromOffset + "->" + elem.untilOffset)
//      }
      rdd
    })

    //改变日期格式 增加日期和小时字段
    val jObjStream: DStream[JSONObject] = offSetStream.map(record => {
      val value: String = record.value()
      val jObj: JSONObject = JSON.parseObject(value)
      val time: lang.Long = jObj.getLong("ts")
      val dateAndHour: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(time))
      val date: String = dateAndHour.split(" ")(0)
      val hour: String = dateAndHour.split(" ")(1)
      jObj.put("td", date)
      jObj.put("th", hour)
      jObj
    })

    //利用redis进行去重处理
    val dauDStream: DStream[JSONObject] = jObjStream.mapPartitions(iter => {
      val jedis: Jedis = RedisUtil.getJedisClient()
      val objects: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
      for (elem <- iter) {
        val date: String = elem.getString("td")
        val mid: String = elem.getJSONObject("common").getString("mid")
        val isNew: lang.Long = jedis.sadd(date, mid)
        if (isNew == 1L) {
          objects += elem
        }
      }
      println("去重后单个list长度-----------" + objects.size)
      jedis.close()
      objects.toIterator
    })

    //单条插入
//    dauDStream.foreachRDD(rdd => {
//      val dauInfo: RDD[DauInfo] = rdd.map(x => jsonToDauInfo(x))
//      dauInfo.foreach(info => EsUtil.insertDoc(info,"gmall_dau_info_2020_10_01"))
//      OffsetToRedis.saveOffSet(topicName, groupId, ranges)
//    })

    //批量插入
    dauDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jsonList: List[JSONObject] = iter.toList
        var date: String = null
        val tuples: List[(String, DauInfo)] = jsonList.map(x => {
          val info: DauInfo = jsonToDauInfo(x)
          date = info.td
          (info.mid, info)
        })
        EsUtil.bulkDoc(tuples, "gmall_dau_info_"+date)
      })
      OffsetToRedis.saveOffSet(topicName, groupId, ranges)
    })
    context.start()
    context.awaitTermination()
  }

  val jsonToDauInfo = (str: JSONObject) => {
    val common: JSONObject = str.getJSONObject("common")
    val info: DauInfo = DauInfo(common.getString("ar"),
      common.getString("ba"),
      common.getString("ch"),
      common.getString("md"),
      common.getString("mid"),
      common.getString("os"),
      common.getString("uid"),
      common.getString("vc"),
      str.getString("td"),
      str.getString("th"),
      str.getLong("ts"))
    info
  }
}


