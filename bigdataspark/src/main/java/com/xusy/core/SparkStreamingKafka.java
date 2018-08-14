package com.xusy.core;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/***
 * @Author: xushiyong
 * @Description
 * @Date: Created in 10:13 2018/6/29
 * @Modify By:
 **/
public class SparkStreamingKafka {
    public static void main(String[] args){
        SparkConf conf = new SparkConf() ;

        SparkSession ss = SparkSession
                .builder()
                .appName("spark-kafka-test")
                .master("local")
                .config(conf)
                .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(ss.sparkContext()), Durations.seconds(60));

        //设置Kafka连接参数
        final HashMap<String,Object> kafkaParams = new HashMap<String, Object>() ;
        kafkaParams.put("bootstrap.servers", "sjzxhdp04:9092,sjzxhdp05:9092,sjzxhdp06:9092,sjzxhdp07:9092,sjzxhdp08:9092,sjzxhdp09:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-group1");
        kafkaParams.put("auto.offset.reset", "earliest"); //默认第一次执行应用程序时从Topic的首位置开始消费
        kafkaParams.put("enable.auto.commit", "false");   //不使用kafka自动提交模式，由应用程序自己接管offset

        Collection<String> topics = Arrays.asList("PMS_GL.ZW_QFJL");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = directStream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.key()+stringStringConsumerRecord.value();
            }
        }) ;
        lines.print();
//        directStream.foreachRDD(rdd ->{
//                if (rdd != null && rdd.partitions().size() > 0) {
//                    rdd.mapPartitions(iter ->{
//                        List<Row> rowList = new ArrayList<>();
//                        while (iter.hasNext()) {
//                            ConsumerRecord<String, String> jsonData = iter.next();
//                            System.out.println(jsonData.value());
//                        }
//                        return rowList.iterator();
//                    }).collect() ;
//                }
//        });




        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
