package dwd

import bean.{OrderInfo, OrderState, ProvinceInfo}
import Utils.{KafkaUtil, OffsetToHdfs, PheonixUtil}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
/**
 * 所有对RDD具体数据的操作都是在executor上执行的 比如map mapPartitions foreach foreachPartition 都是针对RDD内部数据进行处理的 所以我们传递给这些算子的函数都是执行于executor端的
 * 所有对RDD自身的操作都是在driver上执行的 比如foreachRDD transform则是对RDD本身进行一列操作 所以它的参数函数是执行在driver端的
 * 所以这类算子内部是可以使用外部变量 比如在Spark Streaming程序中操作offset 动态更新广播变量等
 */

object OrderInfoApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(4))

    val topic = Array("ODS_ORDER_INFO")
    val groupId: String = "ODS_ORDER_INFO_G0"
    val kafkaParam: Map[String, Object] = Map("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "group.id" -> groupId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest")

    //从hdfs读取偏移量
    val map: collection.Map[TopicPartition, Long] = OffsetToHdfs.getOffset(topic(0), groupId)

    var dStream: InputDStream[ConsumerRecord[String, String]] = null
    //直接读取或者从偏移量处读取
    if (map == null || map.size == 0) {
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topic, kafkaParam))
    } else {
      dStream = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topic, kafkaParam, map))
    }

    //获取本次偏移量
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetStream: DStream[ConsumerRecord[String, String]] = dStream.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val infoStream: DStream[OrderInfo] = offsetStream.transform(rdd => {
      val newRdd: RDD[OrderInfo] = rdd.mapPartitions(iter => {
        val infoes: Iterator[OrderInfo] = iter.map(x => {
          val info: OrderInfo = JSON.parseObject(x.value(), classOf[OrderInfo])
          val create_time: String = info.create_time
          val date: Array[String] = create_time.split(" ")
          info.create_date = date(0)
          info.create_hour = date(1).split(":")(0)
          info
        })
        infoes
      })
      newRdd
    })

    //读取用户状态信息封装到OrderInfo中
    val infoWithFirstDStream: DStream[OrderInfo] = infoStream.mapPartitions(iter => {
      val infoList: List[OrderInfo] = iter.toList
      if (infoList.length > 0) {
        //去phoenix查询状态
        val idList: List[String] = infoList.map(x => x.user_id).map(x => "'" + x + "'")
        val ids: String = idList.mkString(",")
        val sql = "select * from user_state2020 where user_id in (" + ids + ")"
        val stateList: ListBuffer[JSONObject] = PheonixUtil.queryData(sql)
        //当查询有结果时 将状态置入OrderInfo对象中
        if (stateList != null && stateList.size != 0) {
          val stateMap: Map[String, String] = stateList.map(x => (x.getString("USER_ID"), x.getString("IF_CONSUMED"))).toMap
          for (x <- 0 until infoList.length) {
            val info: OrderInfo = infoList(x)
            val state: String = stateMap.getOrElse(info.id.toString, null)
            if (state == null || state.equals("0")) {
              //将没有消费记录的用户的是‘否首单标’标记为是 但是这里有bug 假如一个用户在这一个批次中有两个订单 会出现两个首单标记
              info.if_first_order = "1"
            } else {
              info.if_first_order = "0"
            }
          }
        } else {
          infoList.map(x => {
            x.if_first_order = "1"
            x
          })
        }
      }
      infoList.toIterator
    })

    //处理统一批次中相同订单 都会被置为首单的bug
    val sameUserIdDStream: DStream[(Long, Iterable[OrderInfo])] = infoWithFirstDStream.map(info => (info.user_id, info)).groupByKey()

    /**
     * 当调用的函数有两个及其以上的参数的时候，这时候你只能用小括号。
     * 当调用的函数只有一个函数的时候，花括号和小括号都可以使用。但是还有区别的。
     * 如果使用小括号，意味着你告诉编译器：它只接受单一的一行，因此，如果你意外地输入2行或更多，编译器就会报错。但对花括号来说则不然，它可以接受多行的输入。foreachRDD和foreachPartition就是例子。
     * 在调用一个单一参数的函数的时候，如果参数是用case实现的偏函数，那么你只能使用花括号。
     */
    val bugFixedDStream: DStream[OrderInfo] = sameUserIdDStream.flatMap {
      case (userid, iter) => {
        var infoList: List[OrderInfo] = iter.toList
        if (infoList.size > 1) {
          val orderInfoes: List[OrderInfo] = infoList.sortWith((info1, info2) => info1.create_time < info2.create_time)
          val firstOrder: OrderInfo = orderInfoes(0)
          if (firstOrder.if_first_order == 1) {
            for (x <- 1 until orderInfoes.length) {
              orderInfoes(x).if_first_order = "0"
            }
          }
          infoList = orderInfoes
        }
        infoList
      }
    }

    //将省市信息合并
    val provinceInfoStream: DStream[OrderInfo] = bugFixedDStream.transform(rdd => {
      val sql = "SELECT * FROM province_info"
      val provinceList: ListBuffer[JSONObject] = PheonixUtil.queryData(sql)
      val provinceMap: Map[String, JSONObject] = provinceList.map(obj => (obj.getString("ID"), obj)).toMap
      val bcMap: Broadcast[Map[String, JSONObject]] = context.sparkContext.broadcast(provinceMap)
      val rddWithProvinceInfo: RDD[OrderInfo] = rdd.map(info => {
        val provinceMap: Map[String, JSONObject] = bcMap.value
        val provinceInfo: JSONObject = provinceMap.getOrElse(info.province_id.toString, null)
        info.province_name = provinceInfo.getString("NAME")
        info.province_area_code = provinceInfo.getString("AREA_CODE")
        info.province_iso_code = provinceInfo.getString("ISO_CODE")
        info
      })
      rddWithProvinceInfo
    })

    val finalStream: DStream[OrderInfo] =
      provinceInfoStream.transform(rdd => {
      val userInfoList: ListBuffer[JSONObject] = PheonixUtil.queryData("select * from user_info")
      val userInfoMap: Map[String, JSONObject] = userInfoList.map(x => (x.getString("ID"), x)).toMap
      val rddWithUserInfo: RDD[OrderInfo] = rdd.map(info => {
        val userInfo: JSONObject = userInfoMap.getOrElse(info.user_id.toString, null)
        info.user_age_group = userInfo.getString("AGE_GROUP")
        info.user_gender = userInfo.getString("GENDER_NAME")
        info
      })
      rddWithUserInfo
    })

    //更新hbase中用户的首单状态
    finalStream.foreachRDD(rdd => {
      rdd.foreach(info => {
        KafkaUtil.kafkaSink("DWD_ORDER_INFO", null, JSON.toJSONString(info, SerializerFeature.PrettyFormat))
      })
      val hbaseRDD: RDD[OrderState] = rdd.filter(info => info.if_first_order == "1").map(info => OrderState(info.user_id.toString, info.if_first_order))
      hbaseRDD.saveToPhoenix("user_state2020", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop01,hadoop02:2181"))
      //保存偏移量
      OffsetToHdfs.saveOffset(ranges, topic(0), groupId)
    })

    context.start()
    context.awaitTermination()
  }
}
