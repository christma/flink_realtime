package com.cn.pro;

import com.cn.pvuv.entity.RealStatResult;
import lombok.SneakyThrows;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.cn.pvuv.config.ParamConstant.STAT_TYPE_PV_;

public class FiveMinPvOneMinShowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);


        SingleOutputStreamOperator<BehaviorEntity> dataStream = env
                .addSource(new EventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<BehaviorEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<BehaviorEntity>() {
                            @Override
                            public long extractTimestamp(BehaviorEntity element, long recordTimestamp) {
                                return element.getVisitDateTime();
                            }
                        }));


//        dataStream.print();

        dataStream.keyBy(BehaviorEntity::getEventId).process(new MyProcessFunction()).print("res: ");


        env.execute();
    }

    private static class MyProcessFunction extends KeyedProcessFunction<String, BehaviorEntity, String> {

        // uk 时间节点，uv 是pv值
        MapState<Long, Long> mapState;
        ValueState<Long> currentWindowState;
        ValueState<Long> currentComputationState;

        private final Time step = Time.minutes(1);
        private final Time endStep = Time.minutes(5);

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("pv_map_state_desc", Long.class, Long.class));
            currentWindowState = getRuntimeContext().getState(new ValueStateDescriptor<>("current_window_state", Long.class));
            currentComputationState = getRuntimeContext().getState(new ValueStateDescriptor<>("current_computation_state", Long.class));
        }

        @Override
        public void processElement(BehaviorEntity value, KeyedProcessFunction<String, BehaviorEntity, String>.Context ctx, Collector<String> out) throws Exception {

            Long timestamp = value.getVisitDateTime();
            // 如果是 map 是 null 的话 说明是第一条数据，或者 是 下一个窗口的第一条数据
            long stepMilliseconds = step.toMilliseconds();
            long endStepMilliseconds = endStep.toMilliseconds();
            //需要初始化
            if (mapState.isEmpty()) {

                long startTime = timestamp / endStepMilliseconds * endStepMilliseconds;
                initMapState(startTime, ctx);
                currentComputationState.update(startTime);
                currentWindowState.update(startTime);
            }

            long currentWatermark = ctx.timerService().currentWatermark();

            boolean overWindow = false;
            if (currentWatermark != Long.MAX_VALUE && currentWatermark != Long.MIN_VALUE) {
                long watermarkWindowMin = currentWatermark / endStepMilliseconds * endStepMilliseconds;
                if (watermarkWindowMin > currentWindowState.value()) {
                    overWindow = true;
                    currentWindowState.update(watermarkWindowMin);
                    completeMapStats(watermarkWindowMin, ctx);
                }

            }

            if (overWindow) {
                Iterator<Long> iterator = mapState.keys().iterator();
                ArrayList<Long> mapStateKeys = new ArrayList<>();
                while (iterator.hasNext()) {
                    mapStateKeys.add(iterator.next());
                }
                for (Long key: mapStateKeys) {
                    if (key < currentWindowState.value())
                        mapState.remove(key);
                }
            }

            long keyTime = timestamp / stepMilliseconds * stepMilliseconds;
            if (keyTime > currentWindowState.value()) {
                Long pv = mapState.get(keyTime);
                if (pv == null) {
                    pv = 0L;
                }
                mapState.put(keyTime, ++pv);
            }


        }

        private void completeMapStats(long watermarkWindowMin, KeyedProcessFunction.Context ctx) throws Exception {
            long startTime = watermarkWindowMin;
            long endTime = watermarkWindowMin + endStep.toMilliseconds() - 1L;
            while (startTime < endTime) {
                if (!mapState.contains(startTime)) {
                    mapState.put(startTime, 0L);
                }
                ctx.timerService().registerEventTimeTimer(startTime + step.toMilliseconds() - 1L);
                startTime = startTime + step.toMilliseconds();
            }
            if (!mapState.contains(endTime)) {
                mapState.put(endTime, 1L);
            }
            ctx.timerService().registerEventTimeTimer(endTime);

        }

        private void initMapState(long startTime, KeyedProcessFunction<String, BehaviorEntity, String>.Context ctx) throws Exception {
            long stepMilliseconds = step.toMilliseconds();
            long endStepMilliseconds = endStep.toMilliseconds();
            long endTime = startTime + endStepMilliseconds;
            while (startTime < endTime) {
                mapState.put(startTime, 0L);
                startTime = startTime + stepMilliseconds;
            }

            for (Long time: mapState.keys()) {
//                System.out.println(time + step.toMilliseconds() - 1L + "注册时间");
                ctx.timerService().registerEventTimeTimer(time + step.toMilliseconds() - 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, BehaviorEntity, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            System.out.println("注册器执行时间：" + timestamp);
            long computationTime = timestamp - step.toMilliseconds() + 1L;
            LocalDateTime localDateTime = new Timestamp(computationTime).toLocalDateTime();

            String statTime = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            pvStat(ctx.getCurrentKey(), statTime, computationTime, localDateTime, out);
        }

        private void pvStat(String currentKey,
                            String statTime,
                            long computationTime,
                            LocalDateTime localDateTime,
                            Collector<String> out) throws Exception {


            List<Long> mapStatList = new ArrayList<Long>();

            for (Long key: mapState.keys()) {
                if (key <= computationTime) {
                    System.out.println("key :" + key);
                    Long mapValue = mapState.get(key);
                    if (mapValue != null) {
                        mapStatList.add(mapValue);
                    }
                }
            }

            Long aLong = mapStatList.stream().filter(Objects::nonNull).reduce(Long::sum).orElseGet(null);

            //            String res = currentKey + " " + String.valueOf(new TimeStamp(statTime)) + " pv :" + aLong;
            System.out.println(currentKey + " " + statTime + " pv " + aLong);
//            out.collect(res);

        }


    }
}
