package com.atguigu.gmall0105.realtime.dwd

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager}
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


    //通知用户状态为订单增加 首单标志

    //维度数据的合并

    //保存用户状态

    //保存订单明细
  }
}
