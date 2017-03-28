package com.landoop.training.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Create topic test with 3 partitions
 *
 * kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic test
 *
 *
 */
public class MyProducerEmptyKeys {

    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i < 100; i++)
            try {

                producer.send(
                        new ProducerRecord<String, String>("test", Integer.toString(i)));

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.close();

    }
}
