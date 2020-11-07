package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author yymstart
 * @create 2020-11-06 11:38
 */
public interface DauMapper {

    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);

}

