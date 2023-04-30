package com.cn.pro;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
        dataStream.keyBy(BehaviorEntity::getDeviceId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .process(new MyProcessWindowFunction())
                .print();


        env.execute("DAUTest");
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<BehaviorEntity, outEntity, String, TimeWindow> {

        private final Time step = Time.minutes(1);

        ValueState<Long> pvValueState;
        ValueState<Long> tsValueState;


        @Override
        public void open(Configuration parameters) throws Exception {
            pvValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("pv_value_state", Long.class));
            tsValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts_value_state", Long.class));
        }

        @Override
        public void process(String s, ProcessWindowFunction<BehaviorEntity, outEntity, String, TimeWindow>.Context context, Iterable<BehaviorEntity> elements, Collector<outEntity> out) throws Exception {

            Long count = pvValueState.value();
            pvValueState.update(count == null ? 1 : count + 1);
            if(tsValueState.value() == null){
                long startTime = context.window().getStart();
                long endTime = context.window().getEnd();
            }
        }
    }
}
