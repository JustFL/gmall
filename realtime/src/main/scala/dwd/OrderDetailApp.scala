package dwd

import Utils.{DStreamUtil, KafkaUtil, PheonixUtil}
import bean.OrderDetail
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable.ListBuffer



object OrderDetailApp {
  def main(args: Array[String]): Unit = {

    val topic = "ODS_ORDER_DETAIL"
    val groupId = "ODS_ORDER_DETAIL_G0"

    val conf: SparkConf = new SparkConf().setAppName("OrderDetailApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupId, context)
    val objStream: DStream[OrderDetail] =
      dStream.map(x => {
      val obj: OrderDetail = JSON.parseObject(x.value(), classOf[OrderDetail])
      obj
    })

    val skuList: ListBuffer[JSONObject] = PheonixUtil.queryData("select * from sku_info")
    val skuMap: Map[String, JSONObject] = skuList.map(obj => (obj.getString("ID"), obj)).toMap
    val bcskuMap: Broadcast[Map[String, JSONObject]] = context.sparkContext.broadcast(skuMap)

    val spuList: ListBuffer[JSONObject] = PheonixUtil.queryData("select * from spu_info")
    val spuMap: Map[String, JSONObject] = spuList.map(obj => (obj.getString("ID"), obj)).toMap
    val bcspuMap: Broadcast[Map[String, JSONObject]] = context.sparkContext.broadcast(spuMap)

    val trademarkList: ListBuffer[JSONObject] = PheonixUtil.queryData("select * from base_trademark")
    val trademarkMap: Map[String, JSONObject] = trademarkList.map(obj => (obj.getString("ID"), obj)).toMap
    val bctrademarkMap: Broadcast[Map[String, JSONObject]] = context.sparkContext.broadcast(trademarkMap)

    val category3List: ListBuffer[JSONObject] = PheonixUtil.queryData("select * from BASE_CATEGORY3")
    val category3Map: Map[String, JSONObject] = category3List.map(obj => (obj.getString("ID"), obj)).toMap
    val bccategory3Map: Broadcast[Map[String, JSONObject]] = context.sparkContext.broadcast(category3Map)

    val finalStream: DStream[OrderDetail] = objStream.map(obj => {
      val sku_id: Long = obj.sku_id
      val skuObj: JSONObject = bcskuMap.value.getOrElse(sku_id.toString, null)
      obj.spu_id = skuObj.getString("SPU_ID").toLong
      obj.tm_id = skuObj.getString("TM_ID").toLong
      obj.category3_id = skuObj.getString("CATEGORY3_ID").toLong

      val spuObj: JSONObject = bcspuMap.value.getOrElse(obj.spu_id.toString, null)
      obj.spu_name = spuObj.getString("SPU_NAME")

      val trademarkObj = bctrademarkMap.value.getOrElse(obj.tm_id.toString, null)
      obj.tm_name = trademarkObj.getString("NAME")

      val category3Obj: JSONObject = bccategory3Map.value.getOrElse(obj.category3_id.toString, null)
      obj.category3_name = category3Obj.getString("NAME")

      obj
    })

    finalStream.foreachRDD(rdd => {
      //这里为什么不能用map？
      rdd.foreach(detail => {
        val value = JSON.toJSONString(detail, SerializerFeature.PrettyFormat)
        println(value)
        KafkaUtil.kafkaSink("DWD_ORDER_DETAIL", null, value)
      })
    })

    context.start()
    context.awaitTermination()
  }
}
