package com.landoop.training.producers;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducerAvro {

    public static void main (String args[]) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        props.put("schema.registry.url", "http://localhost:8081");

        String schemaString = "{\"namespace\": \"customerManagement.avro\", \"type\": \"record\", " +
                "\"name\": \"Customer\"," +
                "\"fields\": [" +
                "{\"name\": \"id\", \"type\": \"int\"}," +
                "{\"name\": \"name\", \"type\": \"string\"}," +
                "{\"name\": \"email\", \"type\": \"string\"}" +
                "]}";

        Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<GenericRecord, GenericRecord>(props);

        Parser parser = new Parser();
        Schema schema = parser.parse(schemaString);

        GenericRecord record = new Record(schema);
        record.put("id", 123);
        record.put("name", "christina");
        record.put("email", "christina@gmail.com");

        producer.send(new ProducerRecord<GenericRecord, GenericRecord>("my-topicAvro2", record));

        producer.flush();
        producer.close();

    }

}
