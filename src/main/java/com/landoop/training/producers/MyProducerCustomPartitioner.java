package com.landoop.training.producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;

public class MyProducerCustomPartitioner {

    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("partitioner.class", "com.landoop.training.producers.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        CustomerService customerService = new CustomerService();

        List<String> users = customerService.findAllUsers();

        for(String user : users) {
            try {

                producer.send(new ProducerRecord<String, String>("argos-training2", user, "Hello " + user));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();

    }
}
