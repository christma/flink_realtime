package com.cn.base;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Event> source = env.addSource(new ClickSource());


        source.filter(new MyFilter()).print();


        env.execute();
    }

    private static class MyFilter implements org.apache.flink.api.common.functions.FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Jack");
        }
    }
}
