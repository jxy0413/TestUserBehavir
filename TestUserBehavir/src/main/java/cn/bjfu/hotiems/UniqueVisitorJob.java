package cn.bjfu.hotiems;

import cn.bjfu.beans.PageViewCount;
import cn.bjfu.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.HashSet;

/**
 * Created by jxy on 2021/4/19 0019 14:11
 */
public class UniqueVisitorJob {
    public static void main(String[] args)throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        //开窗统计
        SingleOutputStreamOperator<PageViewCount> applyStream = dataStream.filter(t -> "pv".equals(t.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        applyStream.print();

        env.execute("uv count job");
    }

    //实现全窗口定义函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            //定义一个Set结构，保存窗口的所有数据UserId,自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for(UserBehavior ub:values){
                uidSet.add(ub.getUserId());
            }
            out.collect(PageViewCount.builder().url("uv").count((long)uidSet.size()).windowEnd(window.getEnd()).build());
        }
    }
}
