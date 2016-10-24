package com.zanox.kafka.highlevelconsumer;

import com.zanox.generic.Processor;
import kafka.consumer.KafkaStream;

public class ConsumerFactory {
    private final Processor processor;

    public ConsumerFactory(Processor p) {
        this.processor = p;
    }

    Consumer create(KafkaStream<byte[], byte[]> stream, int threadNumber) {
        return new com.zanox.generic.Consumer(stream, threadNumber, this.processor);
    }
}
