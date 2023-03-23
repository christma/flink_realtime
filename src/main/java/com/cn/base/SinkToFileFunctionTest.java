package com.cn.base;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


public class SinkToFileFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());


        StreamingFileSink<String> sinkToFile = StreamingFileSink.<String>forRowFormat(new Path("./output/toFile.txt"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy
                        .builder()
                        .withMaxPartSize(1024)
                        .withRolloverInterval(TimeUnit.SECONDS.toSeconds(20))
                        .withInactivityInterval(TimeUnit.SECONDS.toSeconds(10))
                        .build()

                ).build();


        source.map(Event::toString).addSink(sinkToFile);

        env.execute();
    }
}
