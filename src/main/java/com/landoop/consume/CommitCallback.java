package com.landoop.consume;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CommitCallback {

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
      consumer.commitAsync(new OffsetCommitCallback() {
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                               Exception e) {
          if (e != null)
            System.out.println("Commit failed for offsets " + offsets + " " + e);
        }
      });
    }
  }

}