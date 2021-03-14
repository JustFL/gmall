package bean

import scala.beans.BeanProperty

//SELECT id, order_id, sku_id, sku_name, img_url, order_price, sku_num, create_time, source_type, source_id
case class OrderDetail (@BeanProperty id:Long,
                        @BeanProperty order_id: Long,
                        @BeanProperty sku_id: Long,
                        @BeanProperty order_price: Double,
                        @BeanProperty sku_num: Long,
                        @BeanProperty sku_name: String,
                        @BeanProperty create_time: String,

                        @BeanProperty var spu_id: Long = 0L,
                        @BeanProperty var tm_id: Long = 0L,
                        @BeanProperty var category3_id: Long = 0L,
                        @BeanProperty var spu_name: String = null,
                        @BeanProperty var tm_name: String = null,
                        @BeanProperty var category3_name: String = null
)
