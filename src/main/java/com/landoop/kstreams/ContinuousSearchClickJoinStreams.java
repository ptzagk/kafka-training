package com.landoop.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ContinuousSearchClickJoinStreams {

  public static void main(String[] args) throws Exception {

    String brokers = System.getenv("BROKERS");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "search-click-join");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    // Note: To re-run the demo, you need to use the offset reset tool:
    // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


    // Construct a `KStream` from the input topic ""streams-file-input", where message values
    // represent lines of text (for the sake of this example, we ignore whatever may be stored
    // in the message keys).
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> searches = builder.stream("searches");
    KStream<String, String> clicks = builder.stream("clicks");

    KStream<String, String> joined = searches.join(clicks,
            (searchValue, clickValue) -> "search=" + searchValue + " ,click= " + clickValue,
            JoinWindows.of(TimeUnit.SECONDS.toMillis(5)),
            Serdes.String(),
            Serdes.String(),
            Serdes.String());

    joined.to(Serdes.String(), Serdes.String(), "search-click-join");

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // the stream application will be running forever
  }

}
