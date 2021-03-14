package Utils

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

object OffsetToMysql {
  def main(args: Array[String]): Unit = {
    getOffset("T1","G1")
  }

  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jsonList: ListBuffer[JSONObject] = MysqlUtil.queryData("select * from offset where topic = '" + topic + "' and group_id = '" + groupId + "'")
    val offsetMap: Map[TopicPartition, Long] = jsonList.map(json => {
      val topic: String = json.getString("topic")
      val partition_id: Int = json.getIntValue("partition_id")
      val offset: Int = json.getIntValue("offset")
      val topicPartition: TopicPartition = new TopicPartition(topic, partition_id)
      (topicPartition, offset.toLong)
    }).toMap
    offsetMap
  }
}
