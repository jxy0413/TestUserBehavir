package com.atguigu.gmall0105.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.DauInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
  * Created by jxy on 2020/12/17 0017 22:39
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_START"
    val groupId="DAU_GROUP"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic)
    var recordInputStream:InputDStream[ConsumerRecord[String,String]] = null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
             recordInputStream =  MyKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
             recordInputStream =  MyKafkaUtil.getKafkaStream(topic, ssc)
    }

    //添加手动偏移量
    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges = Array.empty[OffsetRange]

    val startupInputGetOffstream = recordInputStream.transform {
      rdd => //driver? excutor?    //周期性的执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }
    //val startupInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc)
    val startLogInfoDStream: DStream[JSONObject] = startupInputGetOffstream.map { record =>
      val startupJson: String = record.value()
      val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
      val ts  = startupJSONObj.getLong("ts")
      startupJSONObj
    }
    //startLogInfoDStream.print(100)

    val dauLoginfoDstream: DStream[JSONObject] = startLogInfoDStream.transform { rdd =>
      println("前：" +  rdd.count())
      val logInfoRdd: RDD[JSONObject] = rdd.mapPartitions { startLogInfoItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val dauLogInfoList = new ListBuffer[JSONObject]
        val startLogList: List[JSONObject] = startLogInfoItr.toList

        for (startupJSONObj <- startLogList) {
          val ts  = startupJSONObj.getLong("ts")
          val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
          val dauKey = "dau:" + dt
          val ifFirst = jedis.sadd(dauKey, startupJSONObj.getJSONObject("common").getString("mid"))
          if (ifFirst == 1L) {
            dauLogInfoList += startupJSONObj
          }
        }
        jedis.close()
        dauLogInfoList.toIterator
      }
      // println("后：" + logInfoRdd.count())
      logInfoRdd
    }

    val dauDstream = dauLoginfoDstream.map { startupJsonObj =>
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

    //dauDstream.print(100)

    dauDstream.foreachRDD{rdd=>
      rdd.foreachPartition{dauInfoItr=>
        val dauInfoWithIdList: List[(String, DauInfo)] = dauInfoItr.toList.map(dauInfo=>(dauInfo.dt+  "_"+dauInfo.mid,dauInfo))
        //println(dauInfoWithIdList)
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkDoc(dauInfoWithIdList,"gmall_dau_info_"+dateStr)
      }
      //偏移量提交区
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    //dauLoginfoDstream.print(100)

    ssc.start()
    ssc.awaitTermination()
//    val jsonObjDstream = recordInputStream.map {
//      record =>
//        val jsonString = record.value()
//        val jsonObj = JSON.parseObject(jsonString)
//
//        val longTime = jsonObj.getLong("ts")
//        val datahourString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(longTime))
//        val dataHour = datahourString.split(" ")
//        jsonObj.put("dt", dataHour(0))
//        jsonObj.put("hr", dataHour(1))
//        jsonObj
//    }
    //去重思路：利用redis保存今天访问过系统的用户清单
    //清单在redis中保存
    //redis : type ? key ? vlaue? (field?score?) (exprie?)

    //      val filteredDstream = jsonObjDstream.map {
    //      jsonObj =>
    //        val dt = jsonObj.getString("dt")
    //        val mid = jsonObj.getJSONObject("common").getString("mid")
    //        val dauKey = "dau:" + dt
    //        val jedis = RedisUtil.getJedisClient
    //        val ifNew = jedis.sadd(dauKey, mid)
    //        if (ifNew == 1L) {
    //          true
    //        } else {
    //          false
    //        }
    //    }
     // println("hh")

  }
}
