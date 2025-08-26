package com.example.eventing.svc01.kafkaprocessor;

import lombok.Getter;

@Getter
public class KafkaReaderConfig {
    private String bootstrapServers;
    private String topic;
    private String groupId;
    private String keyDeserializer;
    private String valueDeserializer;
    private String autoOffsetReset;
    private String enableAutoCommit;

    public KafkaReaderConfig(
            String bootstrapServers,
            String topic,
            String groupId,
            String keyDeserializer,
            String valueDeserializer,
            String autoOffsetReset,
            String enableAutoCommit
    ) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.autoOffsetReset = autoOffsetReset;
        this.enableAutoCommit = enableAutoCommit;
    };

}
