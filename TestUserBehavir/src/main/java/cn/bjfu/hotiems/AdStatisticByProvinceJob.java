package cn.bjfu.hotiems;

import cn.bjfu.beans.AdClickEvent;
import cn.bjfu.beans.AdCountViewByProvince;
import cn.bjfu.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

/**
 * Created by jxy on 2021/4/22 0022 11:00
 */
public class AdStatisticByProvinceJob {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = AdStatisticByProvinceJob.class.getResource("/AdclickLog.csv");

        DataStream<AdClickEvent> adClickEventDataStream = env.readTextFile(resource.getPath())
                .map(line ->{
                    String[] fields = line.split(",");
                    return AdClickEvent.builder().adId(new Long(fields[1]))
                            .userId(new Long (fields[0]))
                            .province(fields[2])
                            .city(fields[3])
                            .timestamp(new Long(fields[4]))
                            .build();
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //???????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<AdClickEvent> filterAdClinkStream = adClickEventDataStream
                .keyBy("userId","adId")
                .process(new FilterBlackListUser(100));  //?????????

        //?????????????????????????????????
        SingleOutputStreamOperator  <AdCountViewByProvince> adCountResultStream = filterAdClinkStream.keyBy(AdClickEvent::getProvince)
                              .timeWindow(Time.hours(1),Time.minutes(5))
                              .aggregate(new AddCountAgg(),new AdCountResult());

        adCountResultStream.print();
        filterAdClinkStream.getSideOutput(new OutputTag<BlackListUserWarning>("blocklist"){}).print("blacklist-user");

        env.execute("ad count by province job");
    }

    //???????????????????????????
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent>{
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound){
            this.countUpperBound = countUpperBound;
        }

        //????????????
        ValueState<Long> countState;
        ValueState<Boolean> isSenState;

        //?????????????????????????????????????????????

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count",Long.class,0L));
            isSenState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent",Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //???????????????????????????????????????????????????????????????????????????count+1 ?????????????????? ????????????????????????
            Long curCount =countState.value();
            //??????????????????????????????
            if(curCount ==0 ){
                Long ts  = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000);
                ctx.timerService().registerEventTimeTimer(ts);
            }
            //??????????????????
            if(curCount >= countUpperBound){
                //??????????????????????????????????????????????????????????????????????????????
                if(!isSenState.value()){
                    isSenState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blocklist"){},
                            new BlackListUserWarning(value.getUserId(),value.getAdId(),"click over"+countUpperBound+"???"));
                }
                return;
            }
            //?????????????????? ???????????????1 ????????????
            countState.update(curCount+1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            //??????????????????
            countState.clear();
            isSenState.clear();
        }
    }


    public static class AddCountAgg implements AggregateFunction<AdClickEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
            return aLong + 1;
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

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince,String, TimeWindow>{

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(AdCountViewByProvince.builder().count(count).province(province).windowEnd(windowEnd).build());
        }
    }

}
