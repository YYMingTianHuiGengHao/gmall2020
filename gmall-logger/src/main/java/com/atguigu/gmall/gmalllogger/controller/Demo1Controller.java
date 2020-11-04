package com.atguigu.gmall.gmalllogger.controller;


import lombok.extern.slf4j.Slf4j;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yymstart
 * @create 2020-11-03 18:51
 */

    //@Controller相当于继承controller方法
    //@RequestMapping("testDemo")和方法的一一映射
    //@ResponseBody告诉spring返回的不是页面,是普通的json对象
    //@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class Demo1Controller {
    @ResponseBody
    @RequestMapping("testDemo")
    public String testDemo(){
        return "hello demo";
    }

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString){
        System.out.println(logString);
        log.info(logString);
        return "Success";
    }
}
