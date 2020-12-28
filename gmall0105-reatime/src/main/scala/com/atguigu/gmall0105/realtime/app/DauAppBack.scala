package com.atguigu.gmall0105.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.DauInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by jxy on 2020/12/18 0018 15:56
  */
object DauAppBack {
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("DauAppBack").setMaster("local[4]").set("spark.testing.memory","2147480000")
     val ssc = new StreamingContext(sparkConf,Seconds(5))

     val topic  = "GMALL_START"
     val startUpInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc)
     val startLogInfoStream = startUpInputDstream.map { record =>
       val startupJson = record.value()
       val startUpJSONObj = JSON.parseObject(startupJson)
       startUpJSONObj
    }
    //startLogInfoStream.print(100)

    val logInfordd = startLogInfoStream.transform {
      rdd =>
        println("前: " + rdd.count())
        val logInfoRdd = rdd.mapPartitions {
          startLogInfoStr =>
            val jedis = RedisUtil.getJedisClient
            val dauLogInfoList = new ListBuffer[JSONObject]
            val startLogList = startLogInfoStr.toList
            for (startupJSONObj <- startLogList) {
              val ts = startupJSONObj.getLong("ts")
              val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
              val dauKey = "dau:" + dt
              val ifFirst = jedis.sadd(dauKey, startupJSONObj.getJSONObject("common").getString("mid"))
              if (ifFirst == 1L) {
                dauLogInfoList += startupJSONObj
              }
            }
            jedis.close()
            dauLogInfoList.iterator
        }
        println("后："+logInfoRdd.count())
        logInfoRdd
    }

    val dauDstream = logInfordd.map { startupJsonObj =>
      val dtHr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startupJsonObj.getLong("ts")))
      val dtHrArr: Array[String] = dtHr.split(" ")
      val dt = dtHrArr(0)
      val timeArr = dtHrArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)
      val commonJSONObj: JSONObject = startupJsonObj.getJSONObject("common")
      DauInfo(commonJSONObj.getString("mid"), commonJSONObj.getString("uid"), commonJSONObj.getString("mid"), commonJSONObj.getString("ch")
        , commonJSONObj.getString("vc"), dt, hr, mi, startupJsonObj.getLong("ts"))
    }

    dauDstream.print(100)

    dauDstream.foreachRDD{rdd=>
      rdd.foreachPartition{dauInfoItr=>
        val dauInfoWithIdList: List[(String, DauInfo)] = dauInfoItr.toList.map(dauInfo=>(dauInfo.dt+  "_"+dauInfo.mid,dauInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkDoc(dauInfoWithIdList,"gmall_dau_info_"+dateStr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
