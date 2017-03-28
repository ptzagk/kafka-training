package com.landoop.training.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MyConsumerStandalone {

    public static void main(String args []) throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor("coyote_test_avro");
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();

        if (partitionInfos != null) {

            for (PartitionInfo partition : partitionInfos)
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));

            //Manually assign partitions
            consumer.assign(partitions);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + " , partition = " + record.partition() +
                            " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
                }
                consumer.commitSync();
            }
        }
    }
}
