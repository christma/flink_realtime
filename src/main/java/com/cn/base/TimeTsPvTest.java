package com.cn.base;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimeTsPvTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );


        source.print("source -> ");

        source.keyBy(x -> x.user)
                .process(new MyProcessFunc())
                .print();


        env.execute();
    }

    private static class MyProcessFunc extends KeyedProcessFunction<String, Event, String> {

        ValueState<Long> pvCountState;
        ValueState<Long> timeTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pvCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Long.class));
            timeTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_ts", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            Long count = pvCountState.value();
            pvCountState.update(count == null ? 1 : count + 1);

            if (timeTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timeTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + " pv : " + pvCountState.value());
            timeTsState.clear();
        }
    }
}
