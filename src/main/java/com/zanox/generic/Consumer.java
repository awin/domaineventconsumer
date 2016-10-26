package com.zanox.generic;

import com.zanox.generic.eventHandler.ErrorInHandlerException;
import com.zanox.generic.parser.BrokenMessageFormatException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class Consumer implements com.zanox.kafka.highlevelconsumer.Consumer {

    private final KafkaStream stream;
    private final int threadNumber;
    private Processor processor;

    public Consumer(KafkaStream<byte[], byte[]> stream, int threadNumber, Processor processor) {
        System.out.println("stream: " + stream);
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.processor = processor;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            byte[] message = it.next().message();

            try {
                processor.process(message);
            } catch (BrokenMessageFormatException e) {
                // Log message and move on
                // ACK
            } catch (ErrorInHandlerException e) {
                // Broken implementation
                // NACK
            }
        }

        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
