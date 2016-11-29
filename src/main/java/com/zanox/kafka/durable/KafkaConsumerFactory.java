package com.zanox.kafka.durable;

import com.zanox.kafka.durable.infrastructure.TopicConsumer;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.List;

public class KafkaConsumerFactory {
    public SimpleConsumer createSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        return new SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
    }

    public TopicConsumer createConsumerForTopic(String topic, List<String> brokers) {
        // Consumer containing all leaders and topics
        return new TopicConsumer(topic, brokers);
    }
}
