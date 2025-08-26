package com.example.eventing.svc01.service;

import com.example.eventing.svc01.fileprocessor.FileChecker;
import com.example.eventing.svc01.fileprocessor.FileReadProcessor;
import com.example.eventing.svc01.kafkaprocessor.KafkaWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class File002ReaderService {

    // Configuration values for File001.
    private String fname;
    private String bootstrapServers;
    private String topic;
    private String keySerializer;
    private String valueSerializer;
    private String acks;
    private FileChecker fileChecker;
    private KafkaWriterConfig kafkaWriterConfig;

    public File002ReaderService(
            @Value("${appConfig.inFileNames.file002}") String fname,
            @Value("${kafka.producers.producer002.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.producers.producer002.topic}") String topic,
            @Value("${kafka.producers.producer002.keySerializer}") String keySerializer,
            @Value("${kafka.producers.producer002.valueSerializer}") String valueSerializer,
            @Value("${kafka.producers.producer002.acks}") String acks
    ) {
        this.fname = fname;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.acks = acks;
        this.fileChecker = new FileChecker(fname);
        this.kafkaWriterConfig = new KafkaWriterConfig(bootstrapServers, topic, keySerializer, valueSerializer, acks);
    }

    @Scheduled(fixedRate = 5000)
    public void watchFile() throws IOException {
        if (fileChecker.fileExists()) {
            log.info("File exists: " + fileChecker.getFileName() + " " + Thread.currentThread().getName());
            FileReadProcessor fileReadProcessor = new FileReadProcessor.Builder().fname(fname).kafkaWriterConfig(kafkaWriterConfig).build();
            fileReadProcessor.processFile();
        } else {
            log.info("File does not exist: " + fileChecker.getFileName() + " " + Thread.currentThread().getName());
        }
    }
}
