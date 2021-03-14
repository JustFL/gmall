package disaster

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

case class Phone(id: Int)

object TestSql {
  def main(args: Array[String]): Unit = {

    val ints: List[Int] = List(1, 2, 3, 4, 5)
    val strings: List[String] = ints.map(x => "'" + x + "'")
    val ids: String = strings.mkString(",")
    val sql = "select * from user_state2020 where user_id in (" + ids +")"
    println(sql)

    for (x <- 0 until strings.length) {
      println(strings(x))
    }

    //1601481967729
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = new Date(1601481983021L)
    val dateString: String = df.format(date)
    println(dateString)

    println(Math.abs("G1".hashCode()) % 4)

    val str1 = "12345"
    println(str1.substring(0,str1.length-1))

    val p1: Phone = new Phone(2)
    val p2: Phone = new Phone(1)
    val l1: List[Phone] = List(p1, p2)
    println(l1)
    l1.sortWith((p1, p2) => p1.id < p2.id)
    println(l1)

    val subStr = "Hello World"
    println(subStr.substring(3))

    val tuples: ListBuffer[(String, (String, String))] = new ListBuffer[(String, (String, String))]
    tuples.append(("a", ("a1", "a2")))

    val d1: Double = 3.5
    val d2: Double = 4.5
    val d3 = d1 * d2
    println(d3)

    println(math.round(44.456*100)/100.0D)
  }
}
