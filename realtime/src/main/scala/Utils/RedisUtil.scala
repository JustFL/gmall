package Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  var pool: JedisPool = null

  def getJedisClient(): Jedis = {
    if (pool == null) {
      val conf: JedisPoolConfig = new JedisPoolConfig
      conf.setMaxTotal(100)  //最大连接数
      conf.setMaxIdle(20)   //最大空闲
      conf.setMinIdle(20)     //最小空闲
      conf.setBlockWhenExhausted(true)  //忙碌时是否等待
      conf.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      conf.setTestOnBorrow(true) //每次获得连接的进行测试
      pool = new JedisPool(conf,"hadoop02", 6379)
    }
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    println(getHashValue)
  }

  def addString() = {
    val jedis: Jedis = getJedisClient()
    for (x <- 1 to 100000) {
      jedis.set("str"+x,x.toString)
    }
    jedis.close
  }

  def addSet() = {
    val jedis: Jedis = getJedisClient()
    for (x <- 1 to 100000) {
      jedis.sadd("set1",x.toString)
    }
    jedis.close
  }

  def addZset() = {
    val jedis: Jedis = getJedisClient()
    for (x <- 1 to 5) {
      jedis.zadd("zset1", x.toDouble, x.toString)
    }
    jedis.close
  }

  def addHash() = {
    val jedis: Jedis = getJedisClient()
    for (x <- 1 to 5) {
      jedis.hset("hash1",x.toString,x.toString)
    }
    jedis.close
  }

  def addList() = {
    val jedis: Jedis = getJedisClient()
    for (x <- 1 to 5) {
      jedis.rpush("list1","hello-"+x.toString)
    }
    jedis.close
  }

  def getHashValue(): String = {
    val jedis: Jedis = getJedisClient()
    val result: String = jedis.hget("hash1", "key1")
    jedis.close()
    result
  }
}
