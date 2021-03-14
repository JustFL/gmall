package dws

import java.lang

import Utils.{DStreamUtil, KafkaUtil, RedisUtil}
import bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OrderDetailWideApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic1 = "DWD_ORDER_INFO"
    val groupId1 = "DWD_ORDER_INFO_G0"
    val infoDStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic1, groupId1, context)
    val infoObjDStream: DStream[OrderInfo] = infoDStream.map(x => JSON.parseObject(x.value(), classOf[OrderInfo]))

    val topic2 = "DWD_ORDER_DETAIL"
    val groupId2 = "DWD_ORDER_DETAIL_GO"
    val detailDStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic2, groupId2, context)
    val detailObjDStream: DStream[OrderDetail] = detailDStream.map(x => JSON.parseObject(x.value(), classOf[OrderDetail]))

    val infoWithkeyDStream: DStream[(Long, OrderInfo)] = infoObjDStream.map(info => (info.id, info)).window(Seconds(10), Seconds(5))
    val detailWithKeyDStream: DStream[(Long, OrderDetail)] = detailObjDStream.map(detail => (detail.order_id, detail)).window(Seconds(10), Seconds(5))

    //info 55 detail 47
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = infoWithkeyDStream.join(detailWithKeyDStream)

    val wideDStream: DStream[(Long, (OrderInfo, OrderDetail))] = joinedDStream.mapPartitions(iter => {
      val jedis: Jedis = RedisUtil.getJedisClient()
      val tuples: ListBuffer[(Long, (OrderInfo, OrderDetail))] = new ListBuffer[(Long, (OrderInfo, OrderDetail))]
      iter.foreach {
        case (orderId, (orderinfo, orderdetail)) => {
          val isNew: lang.Long = jedis.sadd("joined", orderdetail.id.toString)
          if (isNew == 1) {
            tuples.append((orderId, (orderinfo, orderdetail)))
          }
        }
      }
      jedis.close()
      tuples.toIterator
    })

    val wideObjDStream: DStream[OrderDetailWide] = wideDStream.map {
      case (orderId, (orderinfo, orderdetail)) => {
      val wide: OrderDetailWide = new OrderDetailWide(orderinfo, orderdetail)
      wide
      }
    }

    wideObjDStream.foreachRDD(iter => {
      iter.foreach(detailwide => {
        KafkaUtil.kafkaSink("DWD_DETAIL_WIDE", null, JSON.toJSONString(detailwide, true))
      })
    })

    context.start()
    context.awaitTermination()
  }
}
