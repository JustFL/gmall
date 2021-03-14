package dws

import java.util.Properties

import Utils.{DStreamUtil, KafkaUtil, RedisUtil}
import bean.OrderDetailWide
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object CalculateShare {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CalculateShare").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, streaming.Seconds(5))
    val topic = "DWD_DETAIL_WIDE"
    val groupID = "DWD_DETAIL_WIDE_G0"
    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupID, context)

    val wideObjDStream: DStream[OrderDetailWide] = dStream.map(record => {
      val wide: OrderDetailWide = JSON.parseObject(record.value(), classOf[OrderDetailWide])
      wide
    })

    //ADDUP_SHARE 存放实际分摊金额的累计 ADDUP_ORIGIN 存放原始商品价格的累计
    val wideObjWithShareDStream: DStream[OrderDetailWide] = wideObjDStream.map(detailwide => {
      val orderID: Long = detailwide.order_id
      var ADDUP_ORIGIN:Double = 0D
      var ADDUP_SHARE:Double = 0D

      val jedis: Jedis = RedisUtil.getJedisClient()
      //从redis中取出订单中原始金额累计值
      if (jedis.hget("ADDUP_ORIGIN", orderID.toString) != null) {
        ADDUP_ORIGIN = jedis.hget("ADDUP_ORIGIN", orderID.toString).toDouble
      }
      //从redis中取出订单中分摊金额累计值
      if (jedis.hget("ADDUP_SHARE", orderID.toString) != null) {
        ADDUP_SHARE = jedis.hget("ADDUP_SHARE", orderID.toString).toDouble
      }
      //当前商品的总价 = 单价 * 数量
      val current_price: Double = detailwide.sku_num * detailwide.sku_price
      //订单的原始总金额
      val original_total_amount: Double = detailwide.original_total_amount
      //判断是否是订单中最后一个明细
      if (original_total_amount - ADDUP_ORIGIN == current_price) {
        detailwide.final_detail_amount = detailwide.final_total_amount - ADDUP_SHARE
      } else {
        detailwide.final_detail_amount = (math.round(current_price * detailwide.final_total_amount / detailwide.original_total_amount * 100))/100
      }

      jedis.hset("ADDUP_ORIGIN", orderID.toString, (ADDUP_ORIGIN + current_price).toString)
      jedis.hset("ADDUP_SHARE", orderID.toString, (ADDUP_SHARE + detailwide.final_detail_amount).toString)
      jedis.close()
      detailwide
    })

    //写入clickhouse
//    val session: SparkSession = SparkSession.builder().appName("CalculateShare").master("local[4]").getOrCreate()
//    import session.implicits._
//    wideObjWithShareDStream.foreachRDD(rdd => {
//      val df: DataFrame = rdd.toDF()
//      df.write.mode(SaveMode.Append).option("batchsize", "100")
//        .option("isolationLevel", "NONE") // 设置事务
//        .option("numPartitions", "4") // 设置并发
//        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
//        .jdbc("jdbc:clickhouse://hadoop02:8123/default","order_wide",new Properties())
//    })

    wideObjWithShareDStream.foreachRDD(rdd => {
      rdd.foreach(wide => KafkaUtil.kafkaSink("DWD_ORDER_WIDE_WITHSHARE", null, JSON.toJSONString(wide, true)))
    })

    context.start()
    context.awaitTermination()
  }
}
