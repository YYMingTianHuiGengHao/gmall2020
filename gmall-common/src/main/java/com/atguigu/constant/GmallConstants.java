package com.atguigu.constant;

/**
 * @author yymstart
 * @create 2020-11-04 9:53
 */
public class GmallConstants {
    public static final String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";
    public static final String KAFKA_TOPIC_EVENT="GMALL_EVENT";


    //订单表日志主题
    public static final String GMALL_ORDER_INFO = "TOPIC_ORDER_INFO";

    //订单明细表日志主题
    public static final String GMALL_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";

    //用户信息表日志主题
    public static final String GMALL_USER_INFO = "TOPIC_USER_INFO";

    //预警日志ES Index前缀
    public static final String ES_ALERT_INDEX_PRE = "gmall_coupon_alert";

    //销售明细ES Index前缀
    public static final String ES_SALE_DETAIL_INDEX_PRE = "gmall2020_sale_detail";

}
