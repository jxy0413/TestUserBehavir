package cn.bjfu.hotiems;


import cn.bjfu.beans.ItemViewCount;
import cn.bjfu.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * Created by jxy on 2021/4/14 0014 18:53
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据 创建数据流
//        DataStream<String> inputStream =
//                env.readTextFile("F:\\wsy\\1.csv");
        Properties props = new Properties();
        props.put("bootstrap.servers", "Master:9092,Worker1:9092,Worker3:9092,Worker4:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>(
                "hostitems",new SimpleStringSchema(),props
        ));


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
        //分组开窗聚合
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(t -> "pv".equalsIgnoreCase(t.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new ItemCountAgg(),new WindowItemCountResult());

        SingleOutputStreamOperator<String> windowEnd = windowAggStream.keyBy("windowEnd")
                .process(new TopNhotItem(5));

        windowEnd.print("data");

        env.execute("job items analyis");
    }

    public static class TopNhotItem extends KeyedProcessFunction<Tuple,ItemViewCount,String>{
        private Integer topSize;

        public TopNhotItem(int topSize){
            this.topSize = topSize;
        }

        //定义列标状态
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext()
                   .getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> collector) throws Exception {
             itemViewCountListState.add(value);
             ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("======================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n");
            for(int i=0;i<Math.min(topSize,itemViewCounts.size());i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            sb.append("===================\n\n");

            out.collect(sb.toString());
        }
    }


    //自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accmulator) {
            return accmulator+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
                 Long itemId = tuple.getField(0);
                 Long windowEnd = timeWindow.getEnd();
                 Long count = input.iterator().next();
                 out.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }
}
