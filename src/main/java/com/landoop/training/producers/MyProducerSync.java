package com.landoop.training.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyProducerSync {

    public static void main(String args[]) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("retries", "3");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 1000000; i++)
            try {

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i));

                producer.send(record).get();

            } catch (Exception e) {
                e.printStackTrace();
            }
        producer.close();

    }
}