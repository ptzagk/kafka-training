package com.landoop.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class SimpleTextTextProducer {

  public static void main(String args[]) throws InterruptedException {

    String brokers = System.getenv("BROKERS");
    String zookeepers = System.getenv("ZK");

    Random r = new Random();

    Producer myProducer = getTextTextProducer(brokers);
    String topicName = "quotes";
    KafkaTools.createTopic(zookeepers, topicName, 3, 1);

    long counter = 0;
    while (counter < 120) {

      Integer randomInt = r.nextInt(10);

      String value = Arrays.asList(
              "Pinky: Gee, Brain, what do you want to do tonight?",
              "Brain: The same thing we do every night, Pinky - try to take over the world!",
              "Brain: Pinky, are you pondering what I'm pondering?",
              "Pinky: I think so, Brain, but...",
              "Brain: We must prepare for tomorrow night.",
              "Pinky: Why? What are we going to do tomorrow night?",
              "Brain: The same thing we do every night, Pinky - try to take over the world!")
              .get(r.nextInt(6));

      ProducerRecord newRecord = new ProducerRecord<String, String>(topicName, "Key-" + randomInt, value);

      if ((r.nextInt(1 + (int) counter % 100) >= 20) | (counter % 2 == 0)) {
        myProducer.send(newRecord);
        myProducer.flush();
      }
      Thread.sleep(randomInt);
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
