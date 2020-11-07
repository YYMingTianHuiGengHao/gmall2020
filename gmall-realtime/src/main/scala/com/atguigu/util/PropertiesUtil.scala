package com.atguigu.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author yymstart
 * @create 2020-11-04 19:15
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
