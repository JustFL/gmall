package disaster

import java.text.SimpleDateFormat

import bean.OrderInfo
import com.alibaba.fastjson.JSON

object TestJson {
  def main(args: Array[String]): Unit = {
    val str = "{\"delivery_address\":\"第2大街第18号楼5单元647门\",\"consignee\":\"汪蕊\",\"create_time\":\"2020-05-15 05:01:03\",\"order_comment\":\"描述468281\",\"final_total_amount\":11793.00,\"expire_time\":\"2020-05-15 05:16:03\",\"original_total_amount\":21325.00,\"order_status\":\"1005\",\"out_trade_no\":\"257923148551425\",\"user_id\":85,\"img_url\":\"http://img.gmall.com/937346.jpg\",\"province_id\":64,\"feight_fee\":16.00,\"consignee_tel\":\"13995026320\",\"trade_body\":\"联想(Lenovo)拯救者Y7000 英特尔酷睿i7 2019新款 15.6英寸发烧游戏本笔记本电脑（i7-9750H 8GB 512GB SSD GTX1650 4G 高色域等7件商品\",\"id\":3494,\"benefit_reduce_amount\":9548.00,\"operate_time\":\"2020-05-15 05:01:03\"}"
    val info: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
    println(info.create_time)
  }
}
