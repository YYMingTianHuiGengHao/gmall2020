package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constant.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.util.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @author yymstart
 * @create 2020-11-04 19:41
 */
object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费kafka启动主题数据
   val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
/*    //打印value
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreach(record=>{
        println(record.value())
      })
    })*/

    //4.将一行数据转换为样例类对象,并补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //1.获取value
      val value: String = record.value()
      //2.取出时间戳字段
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      //3.将时间戳转换为字符串
      val dateHourStr = sdf.format(new Date(ts))
      //4.给时间字段重新赋值
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      //返回数据
      startUpLog

    })

    //5.根据redis进行跨批次去重
    val filterByRedis: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream,ssc.sparkContext)

/*    //复用的rdd,dstream要做缓存
    startLogDStream.cache()
    //过滤之前的数据是多少
    startLogDStream.count().print()

    //复用的rdd要做缓存
    startLogDStream.cache()
    //过滤之后的数据是多少
    filterByRedis.count().print()*/

    //6.同批次去重(根据Mid)
    val filterdByMid: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedis)

    //    filterdByMid.cache()
    //    filterdByMid.count().print()

    //7.将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    DauHandler.saveMidToRedis(filterdByMid)

    //8.将去重之后的数据明细写入Pheonix
    //写库都用的是foreachrdd
   /* filterdByMid.foreachRDD(rdd => {

      //saveToPhoenix是一个隐式的方法,因为rdd的方法都已经封装好了,想要扩展就需要使用隐式转换
      rdd.saveToPhoenix("GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })*/
    filterdByMid.foreachRDD(rdd => {

      rdd.saveToPhoenix("GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
