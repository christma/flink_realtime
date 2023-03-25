package com.cn.base;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.swing.plaf.IconUIResource;

public class TumblingWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());

        source.print();

        source.assignTimestampsAndWatermarks(new MyWatermarkStrategy())
                .map(new MyMap())
                .keyBy(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new MyReduce())
                .print();


        env.execute();
    }

    private static class MyMap implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event event) throws Exception {
            return Tuple2.of(event.user, 1L);
        }
    }

    private static class MyReduce implements org.apache.flink.api.common.functions.ReduceFunction<Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
            return Tuple2.of(v1.f0, v1.f1 + v2.f1);
        }
    }

    private static class MyWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

            return new CustomBoundedOutOfOrdernessGenerator();
        }

        private class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {
            private final long delayTime = 0L;
            private long maxTs = Long.MIN_VALUE + delayTime + 1L;

            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                maxTs = Math.max(maxTs, event.timestamp);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
            }
        }
    }
}
