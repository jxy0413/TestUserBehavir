package cn.bjfu.hotiems;

import cn.bjfu.beans.ChannelPromotionCount;
import cn.bjfu.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by jxy on 2021/4/22 0022 10:12
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //自定义数据源
        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimeStamp();
                    }
                });
        SingleOutputStreamOperator<ChannelPromotionCount> aggregate = dataStream.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        aggregate.print();


        env.execute("app marketing by channel job");
    }

    public static class MarketingCountResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(ChannelPromotionCount.builder().channel(channel).behavior(behavior).windowEnd(windowEnd).count(count).build());
        }
    }


    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior>{
        boolean running = true;

        //定义用户行为和渠道集合
        List<String> behaviorList = Arrays.asList("CLICK","DOWNLOAD","INSTALL","UNINSTALL");
        List<String> channelList = Arrays.asList("app store","wechat","weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (running){
                //随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                ctx.collect(MarketingUserBehavior.builder().behavior(behavior).userId(id).channel(channel).timeStamp(timestamp).build());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
