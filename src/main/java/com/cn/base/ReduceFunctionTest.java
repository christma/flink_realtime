package com.cn.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.addSource(new ClickSource());
        source.map(x -> x.user).print();

        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = source.map(new MyMap())
                .keyBy(stringLongTuple2 -> stringLongTuple2.f0)
                .reduce(new MyReduce());


        reduce.print();


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
}
