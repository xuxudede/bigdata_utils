package com.xusy.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class consumerKafks2 {

    public static void main(String[] args){
        new consumerKafks2().testKafkaConsumer();
    }


    public void testKafkaConsumer() {
        Properties props = new Properties();

        props.setProperty("group.id", "test-group");
        props.put("bootstrap.servers", "cdh5.csg.com:9092,cdh6.csg.com:9092,cdh7.csg.com:9092,cdh8.csg.com:9092");
        //props.setProperty("zookeeper.connect", "cdh1.csg.com:2181,cdh2.csg.com:2181,cdh3.csg.com:2181");
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));//订阅topic
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                System.out.printf("topic: %s , partition: %d , offset = %d, key = %s, value = %s%n", record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());
            });
        }

    }

}
