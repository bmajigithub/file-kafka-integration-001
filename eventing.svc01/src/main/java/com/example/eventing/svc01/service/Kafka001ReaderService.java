package com.example.eventing.svc01.service;

import com.example.eventing.svc01.fileprocessor.FileChecker;
import com.example.eventing.svc01.kafkaprocessor.KafkaReadProcessor;
import com.example.eventing.svc01.kafkaprocessor.KafkaReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Kafka001ReaderService {

    // Configuration values for Kafka001 Reader.
    private String bootstrapServers;
    private String topic;
    private String groupId;
    private String keyDeserializer;
    private String valueDeserializer;
    private String autoOffsetReset;
    private String enableAutoCommit;
    private String fname;
    private KafkaReaderConfig kafkaReaderConfig;
    private FileChecker fileChecker;

    public Kafka001ReaderService(
            @Value("${kafka.consumers.consumer001.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.consumers.consumer001.topic}") String topic,
            @Value("${kafka.consumers.consumer001.group-id}") String groupId,
            @Value("${kafka.consumers.consumer001.keyDeserializer}") String keyDeserializer,
            @Value("${kafka.consumers.consumer001.valueDeserializer}") String valueDeserializer,
            @Value("${kafka.consumers.consumer001.auto-offset-reset}") String autoOffsetReset,
            @Value("${kafka.consumers.consumer001.enable-auto-commit}") String enableAutoCommit,
            @Value("${appConfig.outFileNames.file001}") String fname
            ) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.autoOffsetReset = autoOffsetReset;
        this.enableAutoCommit = enableAutoCommit;
        this.fname = fname;
        this.kafkaReaderConfig = new KafkaReaderConfig(bootstrapServers, topic, groupId, keyDeserializer, valueDeserializer, autoOffsetReset, enableAutoCommit);
        this.fileChecker = new FileChecker(fname);
    }

    @Scheduled(fixedRate = 180000)
    public void watchKafka() {
        // Deleting output file if it exists.
        FileChecker fileChecker = new FileChecker(fname);
        if (fileChecker.fileExists()) {
            log.info("File exists, hence deleting: " + fileChecker.getFileName() + " " + Thread.currentThread().getName());
            fileChecker.deleteFile();
        }
        KafkaReadProcessor kafkaReadProcessor = new KafkaReadProcessor(kafkaReaderConfig, fname);
        kafkaReadProcessor.processKafka();
    }
}
