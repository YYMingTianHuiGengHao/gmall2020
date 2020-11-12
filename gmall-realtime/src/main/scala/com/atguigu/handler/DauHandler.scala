package com.atguigu.handler

import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author yymstart
 * @create 2020-11-04 22:12
 */
object DauHandler {
  def filterByMid(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {
    //1.转换数据结构log=>(mid_logdate,log)
    val midDateToLogStream = filterByRedis.map(log => {
      ((log.mid, log.logDate), log)
    })
    //2.按照key进行分组
    val midDateToLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogStream.groupByKey()
    midDateToLogDStream.flatMap{case((mid,date),iter)=>{
      iter.toList.sortWith(_.ts<_.ts).take(1)
    }}
  }

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
    //方案一:使用filter单挑数据过滤
/*    val value1: DStream[StartUpLog] = startLogDStream.filter(log => {
      //2.获取连接
      val jedisClient = RedisUtil.getJedisClient
      //3.判断数据是否存在
      val redisKey = s"DAU:${log.logDate}"
      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
      //4.关闭连接
      jedisClient.close()
      //1.返回值
      !boolean
    })
    value1*/

    //方案二：分区内获取连接
/*      val value2: DStream[StartUpLog] = startLogDStream.transform(rdd => {
        rdd.mapPartitions(iter => {
          //a.获取连接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          //b.过滤
          val logs: Iterator[StartUpLog] = iter.filter(log => {
            val redisKey = s"DAU:${log.logDate}"
            !jedisClient.sismember(redisKey, log.mid)
          })
          //c.归还连接
          jedisClient.close()
          //d.返回数据
          logs
        })
      })
    value2*/

    //方案三:一个批次获取一次连接,在driver端获取数据广播至executor端
    val value3: DStream[StartUpLog] = startLogDStream.transform(rdd => {
      //获取连接
      val jedisClient = RedisUtil.getJedisClient
      //获取mid数据
      val mids = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      //归还连接
      jedisClient.close()
      //封装广播变量
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      //操作rdd做去重
      rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })
    })
    value3

  }

  /**
   * 将去重之后的数据中的mid保存到redis
   *
   * @param startLogDStream 经过2次去重之后的数据集
   */
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {

    startLogDStream.foreachRDD(rdd => {

      //使用分区操作,减少连接的获取与释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历写库
        iter.foreach(log => {
          val redisKey = s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //c.归还连接
        jedisClient.close()

      })

    })

  }

}
