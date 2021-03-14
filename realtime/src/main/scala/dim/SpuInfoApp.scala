package dim

import Utils.DStreamUtil
import bean.SpuInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SpuInfoApp {
  def main(args: Array[String]): Unit = {

    val topic = "ODS_SPU_INFO"
    val groupId = "ODS_SPU_INFO_GO"
    val conf: SparkConf = new SparkConf().setAppName("SpuInfoApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupId, context)

    val objDStream: DStream[SpuInfo] = dStream.map(x => JSON.parseObject(x.value(), classOf[SpuInfo]))

    objDStream.foreachRDD(rdd => rdd.saveToPhoenix("SPU_INFO",
      Seq("ID","SPU_NAME","DESCRIPTION","CATEGORY3_ID","TM_ID"),
      new Configuration,
      Some("hadoop01,hadoop02:2181")))

    context.start()
    context.awaitTermination()
  }
}
