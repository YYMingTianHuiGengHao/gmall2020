package com.atguigu.bean

/**
 * @author yymstart
 * @create 2020-11-10 11:02
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
