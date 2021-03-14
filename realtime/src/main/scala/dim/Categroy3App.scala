package dim

import Utils.DStreamUtil
import bean.Category3
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Categroy3App {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Categroy3App").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "ODS_BASE_CATEGORY3"
    val groupId = "ODS_BASE_CATEGORY3_G0"
    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupId, context)
    val objStream: DStream[Category3] = dStream.map(x => JSON.parseObject(x.value(), classOf[Category3]))
    objStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("BASE_CATEGORY3", Seq("ID", "NAME", "CATEGORY2_ID"), new Configuration, Some("hadoop01,hadoop02:2181"))
    })

    context.start()
    context.awaitTermination()
  }
}
