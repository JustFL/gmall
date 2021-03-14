package disaster

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object TestFlatMap {
  def main(args: Array[String]): Unit = {
    //RDD[(Long, Iterable[(Long, OrderInfo)])]
    val t1: List[(Int, List[Int])] = List((1, List(1, 2, 3)), (2, List(3, 4, 5)))
    val l1: List[List[Int]] = List(List(1, 2, 3), List(6, 5, 4))
    val conf: SparkConf = new SparkConf().setAppName("TestFlatMap").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(Int, List[Int])] = context.makeRDD(t1)
    val rdd2: RDD[List[Int]] = context.makeRDD(l1)

    rdd1.flatMap(x=>x._2).foreach(println)
    println("--------------------")


    rdd2.flatMap(x=>x).foreach(println)


    context.stop
  }

}
