package ads

import java.util.Date
import java.text.SimpleDateFormat

import Utils.{DStreamUtil, OffsetToMysql}
import bean.OrderDetailWide
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

object SpuAmountSumApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SpuAmountSumApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "DWD_ORDER_WIDE_WITHSHARE"
    val groupId = "DWD_ORDER_WIDE_WITHSHARE_G0"
    val partitionToLong: Map[TopicPartition, Long] = OffsetToMysql.getOffset(topic, groupId)
    var dStream: InputDStream[ConsumerRecord[String, String]]  = null
    if (partitionToLong!= null && partitionToLong.size > 0) {
      dStream = DStreamUtil.createDStream(topic, groupId, context, partitionToLong)
    } else {
      dStream = DStreamUtil.createDStream(topic, groupId, context)
    }

    //获取本次偏移量
    var offset: Array[OffsetRange] = Array.empty
    val offsetStream: DStream[ConsumerRecord[String, String]] = dStream.transform(rdd => {
      offset = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val objStream: DStream[OrderDetailWide] = offsetStream.mapPartitions(iter => {
      val wides: ListBuffer[OrderDetailWide] = new ListBuffer[OrderDetailWide]
      for (elem <- iter) {
        val wide: OrderDetailWide = JSON.parseObject(elem.value(), classOf[OrderDetailWide])
        wides.append(wide)
      }
      wides.toIterator
    })

    val reduceStream: DStream[(String, Double)] = objStream.map(wide => (wide.spu_id + ":" + wide.spu_name, wide.final_detail_amount)).reduceByKey(_+_)

    reduceStream.foreachRDD(rdd => {
      //提取所有数据到driver
      val tuples: Array[(String, Double)] = rdd.collect()
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      if (tuples != null && tuples.size > 0) {
        DBs.setup()
        DB.localTx(implicit session => {
          for (elem <- tuples) {
            val amount: Double = elem._2
            val spuId: String = elem._1.split(":")(0)
            val spuName: String = elem._1.split(":")(1)
            val dateTime: String = df.format(new Date)
            SQL("insert into spu_order_stat (stat_time, spu_id, spu_name, amount) values (?,?,?,?)").bind(dateTime, spuId, spuName, amount).update().apply()
          }

          for (elem <- offset) {
            println(elem.partition + "-" + elem.untilOffset)
            SQL("replace into offset (topic, group_id, partition_id, offset) values (?,?,?,?)").bind(topic, groupId, elem.partition, elem.untilOffset).update().apply()
          }

        })
      }
    })

    context.start()
    context.awaitTermination()
  }
}
