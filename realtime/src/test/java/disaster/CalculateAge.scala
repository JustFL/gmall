package disaster

import java.text.SimpleDateFormat
import java.util.Date

object CalculateAge {
  def main(args: Array[String]): Unit = {
    println(getAgeGroup("2017-10-30"))
  }

  def getAgeGroup(birthday: String): Long = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val age: Long = (new Date().getTime - df.parse(birthday).getTime) /1000 / 60 / 60 / 24 / 365
    age
  }
}
