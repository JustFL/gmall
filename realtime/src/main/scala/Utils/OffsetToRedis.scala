package Utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetToRedis {

  def getOffSet(topic:String, groupid:String): Map[TopicPartition,Long] = {

    val jedis: Jedis = RedisUtil.getJedisClient()
    val hkey = topic+":"+groupid
    //Map[partitionid,offset]
    val offsetMap: util.Map[String, String] = jedis.hgetAll(hkey)

    offsetMap.forEach((x,y) => {
      println("从redis获取的分区编号是:" + x + "偏移量为:" + y)
    })

    jedis.close()
    import scala.collection.JavaConverters._
    val scalaMap: mutable.Map[String, String] = offsetMap.asScala
    val partitionOffset: Map[TopicPartition, Long] = scalaMap.map(x => (new TopicPartition(topic, x._1.toInt), x._2.toLong)).toMap
    partitionOffset
  }

  def saveOffSet(topic:String, groupid:String, ranges:Array[OffsetRange]) = {
    val jedis: Jedis = RedisUtil.getJedisClient()
    val hkey = topic+":"+groupid
    for (elem <- ranges) {
      jedis.hset(hkey,elem.partition.toString,elem.untilOffset.toString)
      println(elem.partition + "号分区偏移量" + elem.untilOffset + "写入redis")
    }
    println("--------------------------------")
    jedis.close()
  }
}

