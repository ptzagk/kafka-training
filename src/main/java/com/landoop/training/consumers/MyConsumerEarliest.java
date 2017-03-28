package com.landoop.training.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyConsumerEarliest {

    public static void main(String args []) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "my-consumer10");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);

        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(500);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + " , partition = " + record.partition() +
                            " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}