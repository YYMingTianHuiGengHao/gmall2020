package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constant.GmallConstants
import com.atguigu.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author yymstart
 * @create 2020-11-10 18:17
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_USER_INFO,ssc)

    //取出value
    val userJsonDStream: DStream[String] = kafkaDStream.map(_.value())

    //将用户数据写入redis
    userJsonDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.写库
        iter.foreach(userJson => {
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          val redisKey = s"UserInfo:${userInfo.id}"
          jedisClient.set(redisKey, userJson)
        })
        //c.归还连接
        jedisClient.close()
      })

    })

    //打印测试
/*
    kafkaDStream.foreachRDD(
      rdd=>{
        rdd.foreach(record=>println(record.value()))
      }
    )
    kafkaDStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(iter=>{
          iter.foreach(record=>{
            println(record.value())
          })
        })
      }
    )
*/


    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
