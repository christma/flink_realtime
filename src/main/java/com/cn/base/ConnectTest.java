package com.cn.base;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source1 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        source1.map(x -> x.timestamp).print("source1:");

        SingleOutputStreamOperator<Event> source2 = env.addSource(new ClickSource2())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        source2.map(x -> x.timestamp).print("source2:");
        source1.connect(source2).process(new CoProcessFunction<Event, Event, String>() {
            @Override
            public void processElement1(Event value, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("水位线1： " + ctx.timerService().currentWatermark() + "  值为 " + value.user);
            }

            @Override
            public void processElement2(Event value, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("水位线2： " + ctx.timerService().currentWatermark() + "  值为 " + value.user);
            }
        }).print();


        env.execute();
    }
}
