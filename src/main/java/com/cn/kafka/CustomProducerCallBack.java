package com.cn.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBack {
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
            producer.send(new ProducerRecord<>("mock", "hello world " + i++), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println(recordMetadata.partition()+"  "+ recordMetadata.offset());
                    }
                }
            });
            Thread.sleep(1000);
        }


    }
}
