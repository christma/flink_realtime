package com.cn.base;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapRichFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());


        source.map(new MyRichMap()).print();


        env.execute();


    }

    private static class MyRichMap extends RichMapFunction<Event, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 方法声明 " + getRuntimeContext().getIndexOfThisSubtask());
        }


        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close 方法声明 " + getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
