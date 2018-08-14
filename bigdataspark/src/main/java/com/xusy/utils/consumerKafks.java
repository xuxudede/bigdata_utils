package com.xusy.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class consumerKafks {

    public static void main(String[] args){
        new consumerKafks().testKafkaConsumer();
    }


    public void testKafkaConsumer() {
        Properties props = new Properties();

        props.setProperty("group.id", "test-group");
        props.put("bootstrap.servers", "sjzxhdp04:9092,sjzxhdp05:9092,sjzxhdp06:9092,sjzxhdp07:9092,sjzxhdp08:9092,sjzxhdp09:9092");
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
