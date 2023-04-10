package com.cn.base;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class FakeWindowsTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        source.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.url + " 时间 : " + new Timestamp(value.timestamp);
            }
        }).print();

        source.keyBy(data -> data.url).process(new FakeWindowProcess(10000L)).print();

        env.execute();
    }

    private static class FakeWindowProcess extends KeyedProcessFunction<String, Event, String> {
        private Long windowSize;

        private MapState<Long, Long> windowCountMapState;

        public FakeWindowProcess(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("my_state", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            Long startWindow = value.timestamp / windowSize * windowSize;
            Long endWindow = startWindow + windowSize;

            ctx.timerService().registerEventTimeTimer(endWindow - 1);

            if (windowCountMapState.contains(startWindow)) {
                Long count = windowCountMapState.get(startWindow);
                windowCountMapState.put(startWindow, count + 1);
            } else {
                windowCountMapState.put(startWindow, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            long endWindow = timestamp + 1;
            long startWindow = endWindow - windowSize;
            long count = windowCountMapState.get(startWindow);

            out.collect(" 窗口 ：" + new Timestamp(startWindow) + " ~~ " + new Timestamp(endWindow) + " key : " + ctx.getCurrentKey() + ",  count : " + count);

            windowCountMapState.remove(startWindow);
//            windowCountMapState.clear();
        }
    }
}
