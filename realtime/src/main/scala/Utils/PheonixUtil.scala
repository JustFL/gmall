package Utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson._
import com.alibaba.fastjson.serializer.SerializerFeature

import scala.collection.mutable.ListBuffer

object PheonixUtil {

  def main(args: Array[String]): Unit = {
    val buffer: ListBuffer[JSONObject] = queryData("select * from T1")
    println(buffer)
  }

  def queryData(sql :String) : ListBuffer[JSONObject] = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop02")
    val statement: PreparedStatement = conn.prepareStatement(sql)

    val buffer: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    val resultSet: ResultSet = statement.executeQuery()
    val metaData: ResultSetMetaData = statement.getMetaData
    val columnCount: Int = metaData.getColumnCount

    while (resultSet.next()) {
      val obj: JSONObject = new JSONObject
      for (x <- 1 to columnCount){
        obj.put(metaData.getColumnName(x), resultSet.getObject(x))
      }
      buffer += obj
    }
    statement.close
    conn.close()
    buffer
  }
}
