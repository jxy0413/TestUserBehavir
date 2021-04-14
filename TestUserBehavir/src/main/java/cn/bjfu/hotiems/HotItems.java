package cn.bjfu.hotiems;


import cn.bjfu.beans.ItemViewCount;
import cn.bjfu.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by jxy on 2021/4/14 0014 18:53
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据 创建数据流
        DataStream<String> inputStream =
                env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\gmall0105parent\\TestUserBehavir\\src\\main\\resources\\UserBehavior.txt");

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

        env.execute("job items analyis");
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
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {

        }
    }
}
