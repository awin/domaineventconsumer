package com.zanox.kafka.durable.infrastructure;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.List;

public class KafkaConsumerFactory {
    public SimpleConsumer simpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        return new SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
    }

    public TopicConsumer topicConsumer(String topic, List<String> brokers) {
        // Consumer containing all leaders and topics
        return new TopicConsumer(topic, brokers);
    }

    public FetchConsumer fetchConsumer() {
        return new FetchConsumer(this);
    }
}
