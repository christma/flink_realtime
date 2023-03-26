package com.cn.base;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import java.util.HashSet;

public class UVCountTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new ClickSource());


        SingleOutputStreamOperator<Event> stream = source.setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));



        stream.keyBy(data -> true)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .aggregate(new UvAgg(),new UvResult()).print();



        env.execute();

    }

    private static class UvAgg implements AggregateFunction<Event,HashSet<String>,Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    private static class UvResult extends ProcessWindowFunction<Long,String,Boolean,TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = elements.iterator().next();
            out.collect("窗口 "+ new Timestamp(start) + " ~ " + new Timestamp(end) + " UV值为： "+ uv);
        }
    }
}
