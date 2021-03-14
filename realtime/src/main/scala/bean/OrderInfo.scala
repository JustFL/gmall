package bean

import scala.beans.BeanProperty

case class OrderInfo(@BeanProperty id: Long,
                     @BeanProperty province_id: Long,
                     @BeanProperty order_status: String,
                     @BeanProperty user_id: Long,
                     @BeanProperty final_total_amount: Double,
                     @BeanProperty benefit_reduce_amount: Double,
                     @BeanProperty original_total_amount: Double,
                     @BeanProperty feight_fee: Double,
                     @BeanProperty expire_time: String,
                     @BeanProperty create_time: String,

                     @BeanProperty var create_date: String,
                     @BeanProperty var create_hour: String,

                     @BeanProperty var if_first_order:String,
                     @BeanProperty var province_name:String,
                     @BeanProperty var province_area_code:String,
                     @BeanProperty var province_iso_code:String,

                     @BeanProperty var user_age_group:String,
                     @BeanProperty var user_gender:String)
