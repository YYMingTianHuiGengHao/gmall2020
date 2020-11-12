package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @author yymstart
 * @create 2020-11-06 11:47
 */
public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

    String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException;
}
