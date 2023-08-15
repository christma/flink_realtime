package com.cn.base;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;

import java.util.Properties;

/**
 * @Author Lyon
 * @Date 2023/8/15 16:42
 * @Version 1.0
 */

public class CheckpointTEst {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();


        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        env.getCheckpointConfig().setCheckpointStorage("hdfs://localhost:9000/flink/checkpoint/dir");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("auto.offset.reset", "latest");


        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("mock", new SimpleStringSchema(), properties));

        kafkaStream.print();

        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "localhost:9092");
        pro.setProperty("transaction.timeout.ms", "5000");


        FlinkKafkaProducer<String> flink_kafka_sink = new FlinkKafkaProducer<String>(
                "flinktry",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                pro
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


        kafkaStream.addSink(flink_kafka_sink);

        env.execute("SourceKafkaTest");
    }
}
