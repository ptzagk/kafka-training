package com.landoop.consume;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class AsynchronousCommit {

  public static void main(String args[]) throws InterruptedException {

    String topic = "quotes";

    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.99.100:9092");
    props.put("group.id", "QuoteReader");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Collections.singletonList(topic));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println("topic = " + record.topic() + " , partition = " + record.partition() +
                " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
      }
      try {
        consumer.commitAsync();
      } catch (CommitFailedException e) {
        System.out.println("commit failed " + e);
      }
    }
  }

}