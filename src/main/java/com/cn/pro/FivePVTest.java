package com.cn.pro;

import com.cn.base.Event;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

public class FivePVTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(initConfiguration());

        env.setParallelism(1);

        SingleOutputStreamOperator<BehaviorEntity> streamOperator = env.addSource(new EventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<BehaviorEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<BehaviorEntity>() {
                            @Override
                            public long extractTimestamp(BehaviorEntity element, long recordTimestamp) {
                                return element.getVisitDateTime();
                            }
                        }));

//        streamOperator.filter(new FilterFunction<BehaviorEntity>() {
//            @Override
//            public boolean filter(BehaviorEntity value) throws Exception {
//                return value.getUserId().equals("C");
//            }
//        }).print();

        streamOperator.keyBy(datas -> datas.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MyAgg(),new OutResult())

                .print();


        env.execute("FivePVTest");
    }


    private static Configuration initConfiguration() {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8888);
        return conf;
    }


    private static class MyAgg implements AggregateFunction<BehaviorEntity, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(BehaviorEntity value, Long accumulator) {
            accumulator = accumulator + 1;
            return accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class OutResult extends ProcessWindowFunction<Long,PvEntity,String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Long, PvEntity, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<PvEntity> out) throws Exception {
            long startTime = context.window().getStart();
            long endTime = context.window().getEnd();
            Long pv = elements.iterator().next();
            out.collect(new PvEntity(s,new Timestamp(startTime),new Timestamp(endTime),pv));
        }
    }
}
