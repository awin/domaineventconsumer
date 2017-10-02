package com.zanox.kafka.durable.infrastructure;

import com.zanox.kafka.durable.OffsetException;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class MessageConsumer {
    private KafkaConsumerFactory kafkaConsumerFactory;
    private Broker leader;
    private int SO_TIMEOUT = 100000;

    public MessageConsumer(KafkaConsumerFactory kafkaConsumerFactory, Broker leader) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.leader = leader;
    }

    public Iterable<MessageAndOffset> fetch(String topic, int partition, Long offset) {
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer consumer = this.kafkaConsumerFactory.simpleConsumer(
                this.leader.host(), this.leader.port(), SO_TIMEOUT, 64 * 1024, clientName
        );

        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                .addFetch(topic, partition, offset, 100000)
                .build();

        FetchResponse fetchResponse = consumer.fetch(req);
        // @TODO: Don't close the consumer if we are reusing it
        consumer.close();

        if (fetchResponse.hasError()) {
            // Something went wrong!
            short code = fetchResponse.errorCode(topic, partition);
            System.err.println("Error fetching data from the Broker:" + leader.host() + " Reason: " + code);
            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                // We asked for an invalid offset. For simple case ask for the last element to reset
                throw new OffsetException("Offset out of range, Cannot safely continue");
                //continue; // Retry this batch
            }
            // Throw away consumer
            consumer.close();
            throw new RuntimeException("Fatal error");
        }
        ByteBufferMessageSet a = fetchResponse.messageSet(topic, partition);
        /*
        Wait a bit

        This seems to be required to not overwhelm the message consumer
        which ends up throwing uncatchable Scala ClosedChannelException
        if queried without a delay.
         */
        if (a.sizeInBytes() < 1) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return a;
    }
}
