package com.landoop.training.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i=0; i<50; i++){

            try {

               RecordMetadata metadata = producer
                    .send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), "MyStringValue2"))
                    .get();

                System.out.println(metadata.offset() +" "+ metadata.partition()+" "+ metadata.timestamp());


            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }


        }

        producer.close();








//        for(int i = 0; i < 100; i++)
//            try {
//
//                producer.send(
//                        new ProducerRecord<String, String>("my-topic", Integer.toString(i)));
////                        new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
//
                /**
                 * ALSO
                 * public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)
                 * public ProducerRecord(String topic, Integer partition, K key, V value)
                 * public ProducerRecord(String topic, K key, V value)
                 * public ProducerRecord(String topic, V value) {
                 * */
//
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }

        producer.close();

    }
}
