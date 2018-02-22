package com.zanox.kafka.durable.infrastructure;

import com.zanox.kafka.durable.Offset;
import com.zanox.kafka.durable.OffsetException;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Fetch / Offset API
 *
 */
public class FetchConsumer {
    private KafkaConsumerFactory kafkaConsumerFactory;

    public FetchConsumer(KafkaConsumerFactory kafkaConsumerFactory) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    public Offset getOffset(String topic, BrokerEndPoint leader, int partition, Offset time) {
        SimpleConsumer consumer = this.kafkaConsumerFactory.simpleConsumer(

            leader.host(), leader.port(), 100000, 64 * 1024, "offsetLookup"
        );
        long offset = getOffset(consumer, topic, partition, time.value, "offsetLookup");
        return new Offset(offset);
    }

    // @TODO: Change this to work on a list of partitions
    @Deprecated
    private static long getOffset(SimpleConsumer consumer, String topic, int partition,
                                  long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            throw new OffsetException("Error fetching offset data from Broker");
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
