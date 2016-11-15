package com.zanox.kafka.durable;

import kafka.api.*;
import kafka.api.FetchRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer {
    private final Map<Integer, Broker> partitionCache;
    private final List<String> seedBrokers;
    private String topic;

    /**
     * Consumer constructor
     * Requires a list of seed brokers that will be used for bootstrapping
     * You don't have to specify the whole list as they will be auto-discovered
     * @param topic Topic to work on
     * @param seedBrokers List of seed brokers
     */
    public Consumer(String topic, List<String> seedBrokers) {
        this.topic = topic;
        this.seedBrokers = seedBrokers;
        partitionCache = new HashMap<>();
        getLeaders();
    }

    /**
     * Return list of Partition IDs for the given topic
     * @return List of Partitions IDs
     */
    public List<Integer> getAvailablePartitions() {
        List<Integer> list = new ArrayList<>();
        for (Map.Entry<Integer, Broker> partition : this.partitionCache.entrySet()) {
            list.add(partition.getKey());
        }
        return list;
    }

    /**
     * Return batch of messages for these partitions and offsets
     * This is the main operational method of this library, this is how you get your messages
     * Offsets are intrinsically attached to their partitions so they always have to be passed together.
     * Its valid to specify less partitions than are actually available, but other way around throws an exception.
     * @param partitionOffsetMap Partitions and their offets to look at
     * @return List of Messages from all partitions
     */
    public List<Message> getBatchFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        List<Message> list = new ArrayList<>();

        for (Map.Entry<Integer, Long> bit : partitionOffsetMap.entrySet()) {
            // Going to more partitions shouldn't be a problem, reducing number of partitions isn't allowed.
            if (!partitionCache.containsKey(bit.getKey())) {
                throw new RuntimeException("Specified partition doesn't exist in cache");
            }
            Broker leader = partitionCache.get(bit.getKey());
            int partition = bit.getKey();

            String clientName = "Client_" + this.topic + "_" + partition;
            SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 100000, 64 * 1024, clientName);
            long offset;
            if (null == bit.getValue()) {
                offset = getOffset(consumer, this.topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
            } else {
                offset = bit.getValue();
            }

            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .addFetch(this.topic, partition, offset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                // Something went wrong!
                short code = fetchResponse.errorCode(this.topic, partition);
                System.err.println("Error fetching data from the Broker:" + leader.host() + " Reason: " + code);
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    offset = getOffset(consumer, this.topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    System.err.println("Offset out of range, earliest offset is " + offset);
                    throw new RuntimeException("Offset out of range, Cannot safely continue");
                    //continue; // Retry this batch
                }
                // Throw away consumer
                consumer.close();
                // Force new leader lookup.
                getLeaders();
                continue;
            }

            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }
                offset = messageAndOffset.nextOffset(); // This is just `offset + 1L`
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                Message message = new Message();
                message.body = bytes;
                message.partition = partition;
                message.offset = offset;
                list.add(message);
            }
            consumer.close();
        }
        return list;
    }

    private void getLeaders() {
        List<PartitionMetadata> partitions = findPartitionsForTopic(seedBrokers, 9092, topic);
        if (partitions.isEmpty()) {
            System.err.println("Partition list is empty");
            throw new RuntimeException("Partition list is empty");
        }

        partitionCache.clear(); // Is this atomic? Should this be ?
        for (PartitionMetadata partition : partitions) {
            partitionCache.put(partition.partitionId(), partition.leader());
        }
    }

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
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private List<PartitionMetadata> findPartitionsForTopic(List<String> seedBrokers, int port, String topic) {
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    // Only one topic
                    return item.partitionsMetadata();
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to list partitions for [" + topic
                        + "] Reason: " + e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return new ArrayList<>();
    }
}