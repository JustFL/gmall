package dim

import java.text.SimpleDateFormat
import java.util.Date

import bean.UserInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

object UserInfoApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topics: Array[String] = Array("ODS_USER_INFO")
    val groupId = "ODS_USER_INFO_G0"
    val kafkaParam: Map[String, Object] = Map[String, Object]("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "group.id" -> groupId)
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))

    val infoStream: DStream[UserInfo] = dStream.map(x => {
      val value: String = x.value()
      val info: UserInfo = JSON.parseObject(value, classOf[UserInfo])
      info
    })

    //(id varchar PRIMARY KEY, info.gender varchar, info.birthday varchar, age_group varchar, gender_name varchar
    infoStream.foreachRDD(rdd => {
      val rddWithUserInfo: RDD[UserInfo] = rdd.map(info => {
        val birthday: String = info.birthday
        val age: Long = getAgeGroup(birthday)
        if (age < 20) {
          info.age_group = "20岁以下"
        } else if (age > 30) {
          info.age_group = "30岁以上"
        } else {
          info.age_group = "21到30岁之间"
        }

        if (info.gender == "M") {
          info.gender_name = "男"
        } else {
          info.gender_name = "女"
        }
        info
      })


      rddWithUserInfo.saveToPhoenix("USER_INFO",
        Seq("ID", "GENDER", "BIRTHDAY", "AGE_GROUP", "GENDER_NAME"),
        new Configuration,
        Some("hadoop01,hadoop02:2181"))
      }
    )

    //maxwell引导命令
    //maxwell-bootstrap --user maxwell --password maxwell --host hadoop01 --database gmall --table user_info --client_id maxwell1

    context.start()
    context.awaitTermination()
  }


  def getAgeGroup(birthday: String): Long = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val age: Long = (new Date().getTime - df.parse(birthday).getTime) /1000 / 60 / 60 / 24 / 365
    age
  }
}
