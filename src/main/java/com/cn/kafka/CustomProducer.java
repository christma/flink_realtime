package com.cn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建kafka 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // 发送数据
        int i = 1;
        while (true){
            producer.send(new ProducerRecord<>("twopart","hello world " + i++));
            System.out.println("----");
            Thread.sleep(1000);
        }
    }
}
