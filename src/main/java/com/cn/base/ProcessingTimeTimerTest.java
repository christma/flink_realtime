package com.cn.base;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource());

        source.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        long time = ctx.timerService().currentProcessingTime();

                        out.collect(ctx.getCurrentKey() + "  数据到达事件  " + new Timestamp(time));

                        ctx.timerService().registerProcessingTimeTimer(time + 10 * 1000L);

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "  定时器触发  触发事件 " + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
