package com.example.eventing.svc01.kafkaprocessor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaWriter {

    private final KafkaWriterConfig kafkaWriterConfig;
    private static String kafkaTopic;
    private static KafkaProducer<String, String> producer;

    public KafkaWriter(KafkaWriterConfig kafkaWriterConfig) {
        this.kafkaWriterConfig = kafkaWriterConfig;
        init();
    }

    private void init() {
        log.info("Initializing Kafka Producer");
        // Initialize Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaWriterConfig.getBootstrapServers());
        properties.put("key.serializer", kafkaWriterConfig.getKeySerializer());
        properties.put("value.serializer", kafkaWriterConfig.getValueSerializer());
        properties.put("acks", kafkaWriterConfig.getAcks());
        // Create the Kafka producer
        producer = new KafkaProducer<>(properties);
        kafkaTopic = kafkaWriterConfig.getTopic();
        log.info("Kafka Producer Initialized");
    }

    public void write(String message) {
        log.info("Writing to topic: " + kafkaTopic);
        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(kafkaTopic, message);
        // Send data
        producer.send(producerRecord);
    }

    public void close() {
       // flush and close the producer
        producer.flush();
        producer.close();
        log.info("Kafka Producer Closed");
    }
}
