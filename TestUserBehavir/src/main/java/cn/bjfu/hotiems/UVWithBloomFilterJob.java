package cn.bjfu.hotiems;

import cn.bjfu.beans.PageViewCount;
import cn.bjfu.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;
import scala.Int;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jxy on 2021/4/19 0019 14:55
 */
public class UVWithBloomFilterJob {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("F:\\\\wsy\\\\1.csv");

        DataStream<UserBehavior> dataStream = inputStream
                .map(line ->{
                    String[] fields = line.split(",");
                    return UserBehavior.builder()
                            .userId(new Long(fields[0]))
                            .itemId(new Long(fields[1]))
                            .categoryId(new Integer(fields[2]))
                            .behavior(fields[3])
                            .timeStamp(new Long(fields[4])).build();
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimeStamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<PageViewCount> applyStream = dataStream.filter(t -> "pv".equals(t.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());
        env.execute();
    }
    //实现自定义的处理函数
    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior,PageViewCount,TimeWindow> {
        //定义jedis连接和bu
        ShardedJedisPool pool;
        MyBloomFilter myBloomFilter;
        @Override
        public void open(Configuration parameters) throws Exception {
            JedisShardInfo jedisShardInfo1 = new JedisShardInfo("47.92.212.63", 6699);
            jedisShardInfo1.setPassword("bjfu1022");
            List<JedisShardInfo> list = new LinkedList<JedisShardInfo>();
            list.add(jedisShardInfo1);
            pool = new ShardedJedisPool(new JedisPoolConfig(),list);
            myBloomFilter = new MyBloomFilter(1<<29 ); //保存大概64MB大小
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            //将位图和窗口count值全部存入Redis，用WindowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            //把count值存成一个hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();
            //当前的userId
            Long userId = elements.iterator().next().getUserId();
            //计算位图的偏移量
            Long offset = myBloomFilter.hashCode(userId.toString(),61);
            //3.用Redis的getbit命令，判断对应位置的值
            Boolean isExist = pool.getResource().getbit(bitmapKey,offset);
            if(!isExist){
                //如果不存在，对应的位图
                pool.getResource().setbit(bitmapKey,offset,true);
                //更新redis中保存的count值
                Long uvCount = 0L;
                String hget = pool.getResource().hget(countHashName, countKey);
                if(hget != null && !"".equals(uvCount)) {
                    uvCount = Long.parseLong(hget);
                }
                pool.getResource().hset(countHashName,countKey,String.valueOf(uvCount+1));
                    out.collect(new PageViewCount("uv",windowEnd,uvCount+1));
            }
        }
    }

    //自定义数据类型
    public  static class MyTrigger extends Trigger<UserBehavior, TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一条数据来到，直接触发计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    //自定义一个布隆过滤器
    public static class MyBloomFilter{
        //定义位图的大小,一般定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap){
            this.cap = cap;
        }

        //实现一个hash函数
        public Long hashCode(String value, Integer seed){
            Long result = 0L;
            for( int i = 0 ; i < value.length(); i++){
                 result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

}
