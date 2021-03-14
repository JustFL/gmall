package dim

import Utils.DStreamUtil
import bean.SkuInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val topic = "ODS_SKU_INFO"
    val groupid = "ODS_SKU_INFO_G0"

    val conf: SparkConf = new SparkConf().setAppName("SkuInfoApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupid, context)
    val infoStream: DStream[SkuInfo] =
      dStream.map(x => {
      val info: SkuInfo = JSON.parseObject(x.value(), classOf[SkuInfo])
      info
    })

    infoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("SKU_INFO",Seq("ID",
        "SPU_ID",
        "PRICE",
        "SKU_NAME",
        "SKU_DESC",
        "WEIGHT",
        "TM_ID",
        "CATEGORY3_ID",
        "SKU_DEFAULT_IMG",
        "CREATE_TIME"),new Configuration,Some("hadoop01,hadoop02:2181"))
    })


    //maxwell-bootstrap --user maxwell --password maxwell --host hadoop01 --database gmall --table sku_info --client_id maxwell1
    context.start()
    context.awaitTermination()
  }
}
