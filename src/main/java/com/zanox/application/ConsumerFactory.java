package com.zanox.application;

import kafka.consumer.KafkaStream;

public class ConsumerFactory implements com.zanox.kafka.highlevelconsumer.ConsumerFactory {
    @Override
    public Consumer create(KafkaStream<byte[], byte[]> stream, int threadNumber) {
        return new Consumer(stream, threadNumber);
    }
}
