package com.cn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class CustonConsumerSeek {

    public static void main(String[] args) {

        Properties prop = new Properties();


        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "testIn");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("testIn");
        consumer.subscribe(topics);



        // 指定位置消费
        Set<TopicPartition> assignment = consumer.assignment();



        while (assignment.size() == 0){
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        for (TopicPartition topicPartition: assignment) {
            consumer.seek(topicPartition, 600);
        }


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record: records) {
                System.out.println(record.partition() + "   " + record.value());
            }
        }


    }

}
