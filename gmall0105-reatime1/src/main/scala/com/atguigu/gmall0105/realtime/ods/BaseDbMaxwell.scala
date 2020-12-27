package com.atguigu.gmall0105.realtime.ods

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Created by jxy on 2020/12/23 0023 20:54
  */
object BaseDbMaxwell {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BaseDbMaxwell").setMaster("local[4]").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "ODS_DB_GMALL2020_M"
    val groupId = "base_db_maxwell_group"
    val kafkaOffsetMap = OffsetManager.getOffset(groupId,topic)
    var recordInputStream:InputDStream[ConsumerRecord[String,String]] = null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream  = MyKafkaUtil.getKafkaStream(topic,ssc)
    }
    //手动添加偏移量
    var offsetRanges = Array.empty[OffsetRange]
    val startupInputGetOffstream = recordInputStream.transform{
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val jsonObjDstream = startupInputGetOffstream.map { record =>
      val jsonString = record.value()
      val jsonObj = JSON.parseObject(jsonString)
      jsonObj
    }
    jsonObjDstream.foreachRDD{ rdd=>
      //推回kafka
      rdd.foreach{ jsonObj=>
          val jsonString = jsonObj.getString("data")
          val tableName = jsonObj.getString("table")
          val topic = "ODS_"+tableName.toUpperCase
          MyKafkaSink.send(topic,jsonString)
      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
