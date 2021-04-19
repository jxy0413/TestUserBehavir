package cn.bjfu.hotiems;

import cn.bjfu.beans.ItemViewCount;
import cn.bjfu.beans.PageViewCount;
import cn.bjfu.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Created by jxy on 2021/4/16 0016 12:57
 */
public class PageViewJob {
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


        SingleOutputStreamOperator<PageViewCount> aggregate = dataStream
                .filter(t -> "pv".equalsIgnoreCase(t.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2(random.nextInt(100), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());
        //将各分区数据汇总起来
        SingleOutputStreamOperator<PageViewCount> pvResultStream = aggregate.keyBy(PageViewCount::getWindowEnd)
                .process(new TotalCount());
                pvResultStream.print();

        env.execute("");
    }

    public static class TotalCount extends KeyedProcessFunction<Long,PageViewCount,PageViewCount>{
        //定义状态，保存当前的总count值
        ValueState<Long> totalCountState;

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
             totalCountState.update(totalCountState.value()+value.getCount());
             ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            //定时器触发
            Long value = totalCountState.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),value));
            //清空状态
            totalCountState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count",Long.class,0L));
        }
    }


    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer,Long>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1 + aLong;
        }
    }

    public static class PvCountResult implements WindowFunction<Long,PageViewCount,Integer, TimeWindow>{

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
                out.collect(PageViewCount.builder().url(integer.toString()).windowEnd(window.getEnd()).count(input.iterator().next()).build());
        }
    }
}
