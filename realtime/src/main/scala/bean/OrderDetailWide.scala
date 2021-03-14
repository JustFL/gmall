package bean

import scala.beans.BeanProperty

case class OrderDetailWide( @BeanProperty var order_detail_id:Long =0L,
                            @BeanProperty var order_id: Long=0L,
                            @BeanProperty var order_status:String=null,
                            @BeanProperty var create_time:String=null,
                            @BeanProperty var user_id: Long=0L,
                            @BeanProperty var sku_id: Long=0L,
                            @BeanProperty var sku_price: Double=0D,
                            @BeanProperty var sku_num: Long=0L,
                            @BeanProperty var sku_name: String=null,
                            @BeanProperty var benefit_reduce_amount:Double =0D ,
                            @BeanProperty var original_total_amount:Double =0D ,
                            @BeanProperty var feight_fee:Double=0D,
                            @BeanProperty var final_total_amount: Double =0D ,
                            @BeanProperty var final_detail_amount:Double =0D,

                            @BeanProperty var if_first_order:String=null,

                            @BeanProperty var province_name:String=null,
                            @BeanProperty var province_area_code:String=null,

                            @BeanProperty var user_age_group:String=null,
                            @BeanProperty var user_gender:String=null,

                            @BeanProperty var dt:String=null,

                            @BeanProperty var spu_id: Long=0L,
                            @BeanProperty var tm_id: Long=0L,
                            @BeanProperty var category3_id: Long=0L,
                            @BeanProperty var spu_name: String=null,
                            @BeanProperty var tm_name: String=null,
                            @BeanProperty var category3_name: String=null) {
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.order_status = orderInfo.order_status
      this.create_time = orderInfo.create_time
      this.dt = orderInfo.create_date

      this.benefit_reduce_amount = orderInfo.benefit_reduce_amount
      this.original_total_amount = orderInfo.original_total_amount
      this.feight_fee = orderInfo.feight_fee
      this.final_total_amount = orderInfo.final_total_amount


      this.province_name = orderInfo.province_name
      this.province_area_code = orderInfo.province_area_code

      this.user_age_group = orderInfo.user_age_group
      this.user_gender = orderInfo.user_gender

      this.if_first_order = orderInfo.if_first_order

      this.user_id = orderInfo.user_id
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
    if (orderDetail != null) {
      this.order_detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_name = orderDetail.sku_name
      this.sku_price = orderDetail.order_price
      this.sku_num = orderDetail.sku_num

      this.spu_id = orderDetail.spu_id
      this.tm_id = orderDetail.tm_id
      this.category3_id = orderDetail.category3_id
      this.spu_name = orderDetail.spu_name
      this.tm_name = orderDetail.tm_name
      this.category3_name = orderDetail.category3_name

    }
  }
}