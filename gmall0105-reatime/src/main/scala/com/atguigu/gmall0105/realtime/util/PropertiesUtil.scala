package com.atguigu.gmall0105.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * Created by jxy on 2020/12/17 0017 22:39
  */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
       val properties = PropertiesUtil.load("config.properties")
       println(properties.getProperty("kafka.broker.list"))
  }

  def load(properieName:String):Properties = {
      val prop = new Properties();
      prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(properieName),"utf-8"))
      prop
  }
}
