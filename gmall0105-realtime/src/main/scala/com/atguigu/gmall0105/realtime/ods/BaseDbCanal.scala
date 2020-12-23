package com.atguigu.gmall0105.realtime.ods

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jxy on 2020/12/23 0023 17:44
  */
object BaseDbCanal {
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("BaseDbCanal").setMaster("local[4]").set("spark.testing.memory", "2147480000")
     val ssc = new StreamingContext(sparkConf,Seconds(5))
     val topic = "GMALL0105_DB_C"
     val groupId = "base_db_canal_group"
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
         rdd.foreach{
           jsonObj=>
           val jsonArr = jsonObj.getJSONArray("data")
           import scala.collection.JavaConversions._
           for(jsonObj <- jsonArr){
             println(jsonObj.toString)
             //发送数据到Kafka
           }
         }
        OffsetManager.saveOffset(topic,groupId,offsetRanges)
     }
    ssc.start()
    ssc.awaitTermination()
  }
}
