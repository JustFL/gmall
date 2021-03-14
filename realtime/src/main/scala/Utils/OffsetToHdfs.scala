package Utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object OffsetToHdfs {

  def getSystem() : FileSystem = {
    val hdfsConf: Configuration = new Configuration
    val system: FileSystem = FileSystem.get(new URI("hdfs://myha"), hdfsConf, "hadoop")
    system
  }


  def getOffset(topic : String, groupId : String) : collection.Map[TopicPartition, Long] = {

    val partitionToLong: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

    val system: FileSystem = getSystem()
    val path: Path = new Path("/" + topic + "/" + groupId + "/offset")
    var input: FSDataInputStream = null
    try {
      input = system.open(path)
    } catch {
      case ex: java.io.FileNotFoundException => {
        println("没有偏移量文件 重新开始消费")
        return null
      }
    }
    val length: Int = input.available()
    if (length == 0) {
      return partitionToLong
    }
    val data: Array[Byte] = new Array[Byte](length)
    input.read(data)
    val offset: String = new String(data)
    val lines: Array[String] = offset.split("\\n")
    for (line <- lines) {
      println(line)
      val elems: Array[String] = line.split(":")
      val partition: String = elems(0)
      val offset: String = elems(1)
      val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
      partitionToLong.put(topicPartition,offset.toLong)
    }
    println("---------------------")
    partitionToLong
  }

  def saveOffset(ranges : Array[OffsetRange], topic : String, groupId : String) : Unit = {
    val system: FileSystem = getSystem()
    val path: Path = new Path("/" + topic + "/" + groupId + "/" + "offset")
    val output: FSDataOutputStream = system.create(path, true)
    for (elem <- ranges) {
      val data = elem.partition.toString + ":" + elem.untilOffset.toString + "\n"
      //使用writeUTF方法读出的时候有乱码
      output.write(data.getBytes)
    }
    output.close()
    system.close()
  }
}
