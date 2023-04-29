package com.cn.pro;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import java.util.HashSet;
import java.util.List;

public class PvByWindowTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<BehaviorEntity> source = env.addSource(new EventSource());

        SingleOutputStreamOperator<BehaviorEntity> dataStream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<BehaviorEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<BehaviorEntity>() {
                    @Override
                    public long extractTimestamp(BehaviorEntity element, long recordTimestamp) {
                        return Long.valueOf(element.getVisitDateTime());
                    }
                }));
        dataStream.keyBy(BehaviorEntity::getUserId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .aggregate(new MyAgg(), new MyResult())
                .print();


        env.execute("DAUTest");
    }


    private static class MyAgg implements AggregateFunction<BehaviorEntity, List<String>, Long> {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<String> add(BehaviorEntity value, List<String> accumulator) {
            accumulator.add(value.getUserId());
            return accumulator;
        }

        @Override
        public Long getResult(List<String> accumulator) {
            return Long.valueOf(accumulator.size());
        }

        @Override
        public List<String> merge(List<String> a, List<String> b) {
            return null;
        }
    }

    private static class MyResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = elements.iterator().next();
            out.collect("窗口 " + new Timestamp(start) + " ~ " + new Timestamp(end) + "用户ID: " + s + " UV值为： " + uv);
        }
    }
}
