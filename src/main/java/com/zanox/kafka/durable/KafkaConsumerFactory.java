package com.zanox.kafka.durable;

import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaConsumerFactory {
    public SimpleConsumer createSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        return new SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
    }
}
