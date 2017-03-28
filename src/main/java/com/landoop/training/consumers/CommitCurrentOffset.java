package com.landoop.training.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CommitCurrentOffset {

    public static void main(String args[]) throws InterruptedException {

        String topic = "my-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "QuoteReader2");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic = " + record.topic() + " , partition = " + record.partition() +
                        " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
            }

            try {
                //will commit the last offset returned by poll(), so make sure it's done after processing.,
                //blocked until the broker responds to the commit request (limits throughput.
                //consumer.commitSync();

                //drawback a later commit might have succeed.
                //consumer.commitAsync();


                //async -> with callback
                consumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           Exception e) {
                        if (e != null)
                            System.out.println("Commit failed for offsets " + offsets + " " + e);
                    }
                });


            } catch (CommitFailedException e) {
                System.out.println("commit failed " + e.toString());
            }

        }
    }

}