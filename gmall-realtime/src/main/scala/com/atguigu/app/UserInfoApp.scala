package com.atguigu.app

import com.atguigu.constant.GmallConstants
import com.atguigu.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yymstart
 * @create 2020-11-10 18:17
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_USER_INFO,ssc)

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
