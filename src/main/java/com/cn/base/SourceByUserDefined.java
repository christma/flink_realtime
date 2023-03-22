package com.cn.base;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceByUserDefined {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());


        eventDataStreamSource.print();


        env.execute("SourceByDefined");
    }
}
