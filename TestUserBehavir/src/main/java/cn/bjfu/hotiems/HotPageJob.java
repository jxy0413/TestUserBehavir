package cn.bjfu.hotiems;

import cn.bjfu.beans.ApacheLogEvent;
import cn.bjfu.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by jxy on 2021/4/16 0016 10:02
 */
public class HotPageJob {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\BaiduNetdiskDownload\\Data\\apache.log");

        DataStream<ApacheLogEvent> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    long timeStamp = simpleDateFormat.parse(fields[3]).getTime();
                    return ApacheLogEvent.builder().
                            ip(fields[0]).userId(fields[1])
                            .timeStamp(timeStamp)
                            .method(fields[5])
                            .url(fields[6]).build();
                }
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimeStamp();
            }
        });

        //定义一个测输出流标签
        //OutputTag<ApacheLogEvent> lateTag = new OutputTag<>("late");

        //分组开窗聚合
        DataStream <PageViewCount> windowAggStream = dataStream.filter(t -> "GET".equalsIgnoreCase(t.getMethod()))
                  .keyBy(ApacheLogEvent::getUrl) //按照url分组
                  .timeWindow(Time.minutes(20),Time.minutes(5))
                  .allowedLateness(Time.minutes(1))
                  .aggregate(new PageCountAgg(),new PageCountResult());

        //windowAggStream.print();

        SingleOutputStreamOperator<String> windowStream = windowAggStream.keyBy(PageViewCount::getWindowEnd).process(new TopNhot(3));



        //收集统一窗口count数据，排序输出
        windowStream.print();
        env.execute("hot page job");
    }

    public static class TopNhot extends KeyedProcessFunction<Long,PageViewCount,String> {
        private Integer topSize;

        public TopNhot(Integer topSize){
              this.topSize = topSize;
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> collector) throws Exception {
            pageViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }


        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<PageViewCount>("list-state",PageViewCount.class));
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("======================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n");

            for(int i=0 ;i<Math.min(topSize,pageViewCounts.size());i++){
                PageViewCount currentItemViewCount = pageViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" = ").append(currentItemViewCount.getUrl())
                        .append(" 浏览量 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            sb.append("==============================\n\n");
            out.collect(sb.toString());
        }
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long accmulator) {
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

    public static class PageCountResult implements WindowFunction<Long,PageViewCount,String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
               collector.collect(PageViewCount.builder().url(url).count(iterable.iterator().next()).windowEnd(timeWindow.getEnd()).build());
        }
    }
}
