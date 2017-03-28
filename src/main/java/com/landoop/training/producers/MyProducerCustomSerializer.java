package com.landoop.training.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducerCustomSerializer {

    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "com.landoop.training.producers.CustomerSerializer");

        Producer<Integer, Customer> producer = new KafkaProducer<Integer, Customer>(props);

        Customer customer = new Customer(1, "Christina");

        producer.send(new ProducerRecord<Integer, Customer>("my-topic2", customer.getCustomerId(), customer));

        producer.close();

    }
}
