package com.cn.base;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());


        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );


        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if (event.user.equals("Bob")) {
                    out.collect("this is Bob");
                } else {
                    out.collect(event.user);
                }
            }
        }).print();


        env.execute();

    }
}
