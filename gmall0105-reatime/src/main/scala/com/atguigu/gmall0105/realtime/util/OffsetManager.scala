package com.atguigu.gmall0105.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange


/**
  * Created by jxy on 2020/12/22 0022 9:58
  */
object OffsetManager {
    //从redis中读取偏移量
    def getOffset(topicName:String,groupId:String): Map[TopicPartition,Long] ={
         //Redis中偏移量的保存格式 type?  hash  key? "offset:[topic]:[groupid]"  field? partition_id    value ? offset   expire
         val jedis = RedisUtil.getJedisClient
         val offsetKey = "offset:"+topicName+":"+groupId
         val offsetMap = jedis.hgetAll(offsetKey)
         jedis.close()
         import scala.collection.JavaConversions._
         val kafkaOffsetMap = offsetMap.map {
         case (partitionId, offset) =>
           println("加载分区偏移量："+partitionId+":"+offset)
          (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
         }.toMap
         kafkaOffsetMap
    }

    def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]) = {
        //redis偏移量写入
        val jedis = RedisUtil.getJedisClient
        val offsetKey = "offset:"+topicName+":"+groupId
        val offsetMap:java.util.Map[String,String] = new java.util.HashMap()
        //转换结构
        for(offset<-offsetRanges){
           val partition = offset.partition
           val untilOffset = offset.untilOffset
           offsetMap.put(partition+"",untilOffset+"")
           println("写入分区："+partition+":"+offset.fromOffset+"--->"+offset.untilOffset)
        }
        //写入redis
        if(offsetMap!=null&&offsetMap.size()>0){
          jedis.hmset(offsetKey,offsetMap)
        }
        jedis.close()
    }

}
