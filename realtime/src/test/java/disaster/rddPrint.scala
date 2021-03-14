package disaster

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddPrint {
  def main(args: Array[String]): Unit = {
    val ints: Array[Int] = Array(1, 2, 3, 4)
    val conf: SparkConf = new SparkConf().setAppName("rddPrint").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = context.makeRDD(ints)
    rdd.foreach(x => println(x))


    val map: Map[Int, String] = Map(1 -> "a", 2 -> "b")


    val add = (x:Int) => x + 100
    ints.map(add).foreach(println)
  }
}
