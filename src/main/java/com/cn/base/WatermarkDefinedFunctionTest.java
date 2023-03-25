package com.cn.base;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WatermarkDefinedFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ClickSource()).assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();


        env.execute();

    }

    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;
                }
            };
        }

        private static class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {

            private final Long delayTime = 5000L;
            private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                maxTs = Math.max(event.timestamp, maxTs);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // 发射水位线，默认 200ms
                watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
            }
        }
    }
}
