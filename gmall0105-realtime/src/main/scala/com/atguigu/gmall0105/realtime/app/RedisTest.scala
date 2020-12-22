package com.atguigu.gmall0105.realtime.app

import com.atguigu.gmall0105.realtime.util.RedisUtil

/**
  * Created by jxy on 2020/12/18 0018 14:28
  */
object RedisTest {
  def main(args: Array[String]): Unit = {
     val client = RedisUtil.getJedisClient
     println(client)
  }
}
