package com.cn.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SinkToKafkaFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new ClickSource());


        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");


        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = source.flatMap(new FlatMapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(Event event, Collector<Tuple2<String, Long>> collector) throws Exception {
                        collector.collect(Tuple2.of(event.user, 1L));
                    }
                }).keyBy(x -> x.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });


        reduce.print();

        reduce.map(Tuple2::toString).addSink(new FlinkKafkaProducer<String>("testIn", new SimpleStringSchema(), prop));


        env.execute();

    }
}
