package dim

import Utils.DStreamUtil
import bean.TradeMark
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TradeMarkApp {
  def main(args: Array[String]): Unit = {
    val topic = "ODS_BASE_TRADEMARK"
    val groupId = "ODS_BASE_TRADEMARK_G0"
    val conf: SparkConf = new SparkConf().setAppName("TradeMarkApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val dStream: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(topic, groupId, context)
    val objStream: DStream[TradeMark] = dStream.map(x => JSON.parseObject(x.value(), classOf[TradeMark]))
    objStream.foreachRDD(rdd => {
      val newRDD: RDD[(String, String)] = rdd.map(obj => (obj.tm_id.toString, obj.tm_name))
      //对应列的类型系统即可插入 列名不一定要相同
      newRDD.saveToPhoenix("BASE_TRADEMARK", Seq("ID", "NAME"), new Configuration, Some("hadoop01,hadoop02:2181"))
    })


    //maxwell-bootstrap --user maxwell --password maxwell --host hadoop01 --database gmall --table base_trademark --client_id maxwell1
    context.start()
    context.awaitTermination()
  }
}
