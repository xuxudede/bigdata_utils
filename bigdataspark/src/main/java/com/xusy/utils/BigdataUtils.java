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

public class BigdataUtils {

    public static void main(String[] args){
        new BigdataUtils().testKafkaProducer();
    }


    public void testKafkaProducer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh5.csg.com:9092,cdh6.csg.com:9092,cdh7.csg.com:9092,cdh8.csg.com:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(props);
            //若调用了get，则阻塞直到收到成功响应
//            producer.send(new ProducerRecord<String, String>("test-topic", "a", "777713")).get();
//            producer.send(new ProducerRecord<String, String>("test-topic", "b", "888813")).get();
//            producer.send(new ProducerRecord<String, String>("test-topic", "b", "999913")).get();

            producer.send(new ProducerRecord<>("test-topic", 2,"a","19999237")).get();
            producer.send(new ProducerRecord<>("test-topic", 3,"a","19999337")).get();
            producer.send(new ProducerRecord<>("test-topic", 24,"a","19999437")).get();
            producer.send(new ProducerRecord<>("test-topic", 5,"a","19999537")).get();
            producer.send(new ProducerRecord<>("test-topic", 46,"a","19999637")).get();
            System.out.println("发送成功！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if(producer!=null)
                producer.close();
        }


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
