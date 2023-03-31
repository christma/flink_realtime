package com.cn.base;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutPutFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());

        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob"){};
        OutputTag<Tuple3<String, String, Long>> otherTag = new OutputTag<Tuple3<String, String, Long>>("other"){};


        SingleOutputStreamOperator<Event> outputStreamOperator = source.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (event.user.equals("Bob")) {
                    ctx.output(bobTag, Tuple3.of(event.user, event.user, event.timestamp));
                } else if (event.user.equals("Tom")) {
                    ctx.output(otherTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else {
                    out.collect(event);
                }
            }
        });


        outputStreamOperator.print("main");
        outputStreamOperator.getSideOutput(bobTag).print("bob");
        outputStreamOperator.getSideOutput(otherTag).print("other");

        env.execute();
    }
}
