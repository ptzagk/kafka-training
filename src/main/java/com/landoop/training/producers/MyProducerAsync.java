package com.landoop.training.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyProducerAsync {

    private static class MyProducerCallback implements Callback {

        //@Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++)
            try {

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i));

                producer.send(record, new MyProducerCallback());



            } catch (Exception e) {
                e.printStackTrace();
            }
        producer.close();

    }
}


