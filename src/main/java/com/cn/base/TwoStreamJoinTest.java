package com.cn.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(Tuple3.of("aaa", "stream-01", 1000L), Tuple3.of("bbb", "stream-01", 2000L)).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());


        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(Tuple3.of("aaa", "stream-02", 1000L), Tuple3.of("bbb", "stream-02", 2000L), Tuple3.of("ccc", "stream-02", 3000L)).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        stream1.keyBy(x -> x.f0).connect(stream2.keyBy(x -> x.f0)).process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

            ListState<Tuple3<String, String, Long>> s1ListState;
            ListState<Tuple3<String, String, Long>> s2ListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                s1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("s1", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                s2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("s2", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            }

            @Override
            public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

                if (s2ListState.get().iterator().hasNext()) {
                    for (Tuple3<String, String, Long> right: s2ListState.get()) {
                        out.collect(left + " ---> " + right);
                    }

                } else {
                    out.collect(left.toString());
                }
                s1ListState.add(Tuple3.of(left.f0, left.f1, left.f2));
            }

            @Override
            public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

                if (s1ListState.get().iterator().hasNext()) {
                    for (Tuple3<String, String, Long> left: s1ListState.get()) {
                        out.collect(left + " --- > " + right);
                    }
                } else {
                    out.collect(right.toString());
                }

                s2ListState.add(Tuple3.of(right.f0, right.f1, right.f2));
            }
        }).print();


        env.execute();


    }
}
