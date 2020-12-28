package com.atguigu.gmall0105.realtime.dwd

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Created by jxy on 2020/12/24 0024 21:17
  */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    //加载流
    val sparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "ODS_ORDER_INFO"
    val groupId = "order_info_group"
    val kafkaOffsetMap = OffsetManager.getOffset(groupId,topic)
    var recordInputStream:InputDStream[ConsumerRecord[String,String]] = null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream  = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //手动添加偏移量
    var offsetRanges = Array.empty[OffsetRange]
    val startupInputGetOffstream = recordInputStream.transform{
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    //基本的结构转换，补时间操作
    val jsonObjDstream:DStream[OrderInfo] = startupInputGetOffstream.map { record =>
            val jsonString = record.value()
            val orderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
            val createTimeArr = orderInfo.create_date.split(" ")
            orderInfo.create_date = createTimeArr(0);
            val timeArr:Array[String] = createTimeArr(0).split(":")
            orderInfo.create_hour = timeArr(0)
            orderInfo
    }
    //查询hbase用户状态
    val orderInfoStream = jsonObjDstream.map {
      orderInfo =>
        val sql = "select user_id,if_consumed from USER_STATE0105 where USER_ID = '" + orderInfo.user_id + "'"
        val userStateList = PhoenixUtil.queryList(sql)
        if (userStateList != null && userStateList.size > 0) {
          val userStateJsonObj = userStateList(0)
          if (userStateJsonObj.getString("IF_CONSUMED").equalsIgnoreCase("1")) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        } else {
          orderInfo.if_first_order = ""
        }
    }
    orderInfoStream.print(1000)

    ssc.start()
    ssc.awaitTermination()

//       jsonObjDstream.mapPartitions{ orderInfoItr=>
//            //每分区的操作
//            val orderInfoList = orderInfoItr.toList
//            val userIdList = orderInfoList.map(_.user_id)
//            val sql = "select user_id,if_consumed from USER_STATE0105 where user_id in ( '"+userIdList.mkString("','") +"')"
//            val userStateList = PhoenixUtil.queryList(sql)
//
//            val userStateMap = userStateList.map(jsonObj => (jsonObj.getString("userId"),jsonObj.getString("if_consumed"))).toMap
//
//            null
//       }

    //通知用户状态为订单增加 首单标志

    //维度数据的合并

    //保存用户状态

    //保存订单明细
  }
}
