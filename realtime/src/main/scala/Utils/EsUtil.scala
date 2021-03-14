package Utils

import bean.DauInfo
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Index}

object EsUtil {

  var factory: JestClientFactory = null

  def getClient(): JestClient = {

    if (factory == null) {
      factory = new JestClientFactory
      factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop01:9200").
        maxTotalConnection(50).
        multiThreaded(true).
        connTimeout(10000).
        readTimeout(10000).build())}

    factory.getObject
  }

  //批次操作 传入的时候不仅要传入数据 还要传入es的id 用作幂等性处理
  def bulkDoc(objs: List[(String, Any)], indexName: String): Unit = {
    if (objs.size > 0 && objs != null){
      val jest: JestClient = getClient()
      val builder: Bulk.Builder = new Bulk.Builder()
      for (value <- objs) {
        val elem: Any = value._2
        val id: String = value._1
//        println("id ===== " + id + "            elem =======" + elem)
        val index: Index = new Index.Builder(elem).index(indexName).`type`("_doc").id(id).build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()
      val result: BulkResult = jest.execute(bulk)
      println("ES保存"+result.getItems.size()+"条数据")
      jest.close()
    }
  }


  def insertDoc(obj: DauInfo, indexName: String): Unit = {
    //必须用样例类包装？
    val index: Index = new Index.Builder(obj).index(indexName).`type`("_doc").build()
    val jest: JestClient = getClient()
    val result: DocumentResult = jest.execute(index)
    println(result.getErrorMessage)
    jest.close
  }
}
