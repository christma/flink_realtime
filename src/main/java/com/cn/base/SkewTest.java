package com.cn.base;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class SkewTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setParallelism(3);
        env.disableOperatorChaining();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "mock_consumer01");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("mock", new SimpleStringSchema(), prop);

        SingleOutputStreamOperator streamSource = env.addSource(kafkaSource).setParallelism(1).map(new splitMap()).setParallelism(1);

        SingleOutputStreamOperator<OrderInfo> operator = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        operator.keyBy(OrderInfo::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new MyAgg(), new PrintResult()).print();


        env.execute("SkewTest");
    }


    private static class splitMap implements MapFunction<String, OrderInfo> {

        @Override
        public OrderInfo map(String value) throws Exception {
            JSONObject js = JSONObject.parseObject(value);
            Integer id = js.getInteger("id");
            String gender = js.getString("gender");
            Integer age = js.getInteger("age");
            Long price = js.getLong("price");
            String os = js.getString("os");
            Long ts = js.getLong("ts");
            return new OrderInfo(id, gender, age, price, os, ts);
        }
    }


    private static class MyAgg implements AggregateFunction<OrderInfo, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(OrderInfo value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class PrintResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {

        @Override
        public void process(Integer key, ProcessWindowFunction<Long, String, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Timestamp start = new Timestamp(context.window().getStart());
            Timestamp end = new Timestamp(context.window().getEnd());
            Long cn = elements.iterator().next();

            out.collect("key : " + key + " count: " + cn + " start_time : " + start + " end_time : " + end);
        }
    }
}
