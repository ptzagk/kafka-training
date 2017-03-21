package com.landoop.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class ContinuousSearchClickProducer {

  public static void main(String args[]) throws InterruptedException {

    String brokers = System.getenv("BROKERS");
    String zookeepers = System.getenv("ZK");

    Random r = new Random();

    String topicSearches = "searches";
    String topicClicks = "clicks";

    Producer mySearchesProducer = getTextTextProducer(brokers);
    Producer myClicksProducer = getTextTextProducer(brokers);
    KafkaTools.createTopic(zookeepers, topicSearches, 3, 1);
    KafkaTools.createTopic(zookeepers, topicClicks, 3, 1);

    long counter = 0;
    while (true) {

      Integer randomInt = r.nextInt(100);

      ProducerRecord newClick = new ProducerRecord<String, String>(topicClicks, "user:" + randomInt, "click-x");
      ProducerRecord newSearch = new ProducerRecord<String, String>(topicSearches, "user:" + randomInt, "search for " + randomInt);
      myClicksProducer.send(newClick);
      Thread.sleep(randomInt);
      if (randomInt / 10 == 1) {
        mySearchesProducer.send(newSearch);
      }
      counter++;
      // Log out every 1K messages
      if (counter % 1000 == 0) {
        System.out.print(" . " + (counter / 1000) + "K");
      }

    }
  }

  private static Producer<String, String> getTextTextProducer(String brokers) {
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", brokers);
    producerProps.put("acks", "all");
    producerProps.put("key.serializer", StringSerializer.class.getName());
    producerProps.put("value.serializer", StringSerializer.class.getName());
    producerProps.put("linger.ms", "10");
    return new KafkaProducer<>(producerProps);
  }

}
