package com.cn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class CustomConsumerPartition {
    public static void main(String[] args) {


        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "testIn");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        ArrayList<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

        topicPartitions.add(new TopicPartition("twopart",1));

        consumer.assign(topicPartitions);


        while (true) {
            ConsumerRecords<String, String> polls = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> poll: polls) {
                System.out.println(poll.partition()+"   "+ poll.value());
            }
        }


    }
}
