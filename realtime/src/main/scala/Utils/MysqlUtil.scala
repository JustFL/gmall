package Utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import com.alibaba.fastjson.JSONObject
import scala.collection.mutable.ListBuffer

object MysqlUtil {

  def main(args: Array[String]): Unit = {
    println(queryData("select * from base_category1"))
  }

  def queryData(sql: String): ListBuffer[JSONObject] = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://hadoop01/gmall", "root", "1234")
    val statement: PreparedStatement = connection.prepareStatement(sql)
    val rs: ResultSet = statement.executeQuery()

    val objList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    val metadata: ResultSetMetaData = rs.getMetaData
    val columnCount: Int = metadata.getColumnCount
    while (rs.next()) {
      val obj: JSONObject = new JSONObject()
      for (i <- 1 to columnCount) {
        val colName: String = metadata.getColumnLabel(i)
        metadata.getColumnType(i)
        obj.put(colName, rs.getObject(i))
      }
      objList.append(obj)
    }

    rs.close()
    statement.close()
    connection.close()

    objList
  }
}
