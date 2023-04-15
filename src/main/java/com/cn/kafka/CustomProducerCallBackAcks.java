package com.cn.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class CustomProducerCallBackAcks {
    public static void main(String[] args) throws Exception {


        System.out.println("hello world ".getBytes(StandardCharsets.UTF_8).length);
        System.out.println("hello".getBytes(StandardCharsets.UTF_8).length);
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.RETRIES_CONFIG, 2);
        prop.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10);

        //创建kafka 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // 发送数据
        int i = 1;
        while (true) {
            String str1 = "hello world ";
            String str2 = "hello";
            if (i % 2 == 0) {
                producer.send(new ProducerRecord<>("twopart", str1 + i++), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println(recordMetadata.partition() + "  " + recordMetadata.offset());
                        }
                    }
                });
            } else {
                producer.send(new ProducerRecord<>("testIn", str2 + i++), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println(recordMetadata.partition() + "  " + recordMetadata.offset());
                        }
                    }
                });
            }

            Thread.sleep(100);
        }


    }
}
