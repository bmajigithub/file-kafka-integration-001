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
public class File001ReaderService {

    // Configuration values for File001 Reader.
    private String fname;
    private String bootstrapServers;
    private String topic;
    private String keySerializer;
    private String valueSerializer;
    private String acks;
    private FileChecker fileChecker;
    private KafkaWriterConfig kafkaWriterConfig;

    public File001ReaderService(
            @Value("${appConfig.inFileNames.file001}") String fname,
            @Value("${kafka.producers.producer001.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.producers.producer001.topic}") String topic,
            @Value("${kafka.producers.producer001.keySerializer}") String keySerializer,
            @Value("${kafka.producers.producer001.valueSerializer}") String valueSerializer,
            @Value("${kafka.producers.producer001.acks}") String acks
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

    @Scheduled(fixedRate = 1000)
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
