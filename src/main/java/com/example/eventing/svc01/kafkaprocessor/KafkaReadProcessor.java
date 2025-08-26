package com.example.eventing.svc01.kafkaprocessor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaReadProcessor {

    private final KafkaReaderConfig kafkaReaderConfig;
    private static String fname;
    private static String kafkaTopic;
    private KafkaConsumer<String, String> consumer;
    private final long pollTimeoutMillis = 1000; // Timeout for poll()
    private final int emptyPollsThreshold = 5; // Number of consecutive empty polls to terminate

    public KafkaReadProcessor(KafkaReaderConfig kafkaReaderConfig, String fname) {
        this.kafkaReaderConfig = kafkaReaderConfig;
        this.fname = fname;
        init();
    }

    private void init() {
        log.info("Initializing Kafka Consumer");
        // Initialize Kafka consumer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaReaderConfig.getBootstrapServers());
        properties.put("group.id", kafkaReaderConfig.getGroupId());
        properties.put("key.deserializer", kafkaReaderConfig.getKeyDeserializer());
        properties.put("value.deserializer", kafkaReaderConfig.getValueDeserializer());
        properties.put("auto.offset.reset", kafkaReaderConfig.getAutoOffsetReset());
        properties.put("enable.auto.commit", kafkaReaderConfig.getEnableAutoCommit());
        // Create the Kafka consumer
        consumer = new KafkaConsumer<>(properties);
        kafkaTopic = kafkaReaderConfig.getTopic();
        consumer.subscribe(Collections.singletonList(kafkaTopic));
        log.info("Kafka Consumer Initialized and Subscribed to topic: " + kafkaTopic);
    }

    public void processKafka() {
        log.info("Starting to read from topic: " + kafkaTopic);
        int emptyPollsCount = 0;
        try {
            File file = new File(fname);
            FileWriter fileWriter = new FileWriter(file);
            while (true) {
                // Poll loop to read messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMillis));
                if (records.isEmpty()) {
                    emptyPollsCount++;
                    log.info("No new messages. Empty poll count: " + emptyPollsCount);
                    if (emptyPollsCount >= emptyPollsThreshold) {
                        log.info("No new messages for a while. Exiting read loop.");
                        break;
                    }
                } else {
                    emptyPollsCount = 0; // Reset counter on receiving messages
                    records.forEach(record -> {
                        // Write to the output file
                        try {
                            fileWriter.write(record.value() + System.lineSeparator());
                            log.info("Written message to file: " + record.value());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    consumer.commitSync(); // Commit offsets after processing
                }
            }
            fileWriter.close(); // Close the file writer
            consumer.close(); // Close the consumer
            log.info("Kafka Consumer Closed and File Writer Closed");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
