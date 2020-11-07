package com.atguigu.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author yymstart
 * @create 2020-11-04 19:37
 */
object RedisUtil {

  var jedisPool: JedisPool = _

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数,借了东西要及时还
      jedisPoolConfig.setMaxIdle(20) //最大空闲,没有人连接的时候最大的空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲,没有人连接的时候最小的空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    //因为现在想要看数据条数,每一条打印一次这个就比较烦
//    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
//    println("获得一个连接")
    jedisPool.getResource
  }
}
