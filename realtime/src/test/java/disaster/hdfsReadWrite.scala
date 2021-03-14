package disaster

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object hdfsReadWrite {

  val path: Path = new Path("/GMALL_DB_MAXWELL/MAXWELL-G1/offset")

  def getSystem(): FileSystem = {
    val hdfsConf: Configuration = new Configuration
    FileSystem.get(new URI("hdfs://myha"), hdfsConf, "hadoop")
  }

  def saveToHdfs() = {
    val system: FileSystem = getSystem
    val output: FSDataOutputStream = system.create(path, true)
    output.write(("1:202"+"\n").getBytes)
    output.write(("0:555"+"\n").getBytes)

    output.close()
    system.close()
  }

  def getFromHdfs() = {
    val system: FileSystem = getSystem
    val input:FSDataInputStream = system.open(path)
    val length: Int = input.available()
    val data: Array[Byte] = new Array[Byte](length)
    input.read(data,0, length)
    val offset: String = new String(data)
    val lines: Array[String] = offset.split("\\n")
    println(lines.length)
    for (elem <- lines) {
      val strings: Array[String] = elem.split(":")
      for (elem <- strings) {
        println(elem)
      }
    }

    input.close()
    system.close()
  }

  def main(args: Array[String]): Unit = {
    getFromHdfs
  }
}
