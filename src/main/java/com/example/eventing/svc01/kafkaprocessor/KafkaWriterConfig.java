package com.example.eventing.svc01.kafkaprocessor;

import lombok.Getter;

@Getter
public class KafkaWriterConfig {
    private String bootstrapServers;
    private String topic;
    private String keySerializer;
    private String valueSerializer;
    private String acks;

    public KafkaWriterConfig(String bootstrapServers, String topic, String keySerializer, String valueSerializer, String acks) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.acks = acks;
    };

}
