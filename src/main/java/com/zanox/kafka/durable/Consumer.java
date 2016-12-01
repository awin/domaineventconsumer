package com.zanox.kafka.durable;

import com.zanox.kafka.durable.infrastructure.FetchConsumer;
import com.zanox.kafka.durable.infrastructure.KafkaConsumerFactory;
import com.zanox.kafka.durable.infrastructure.PartitionException;
import com.zanox.kafka.durable.infrastructure.TopicConsumer;
import com.zanox.kafka.durable.infrastructure.PartitionLeader;
import kafka.api.*;
import kafka.api.FetchRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Consumer {
    private final Map<Integer, Broker> partitionCache;
    private final List<String> seedBrokers;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private String topic;

    /**
     * Constructor with Injectable ConsumerFactory
     * @param kafkaConsumerFactory Kafka Consumer factory
     * @param topic Topic to work on
     * @param seedBrokers List of seed brokers
     */
    public Consumer(KafkaConsumerFactory kafkaConsumerFactory, String topic, List<String> seedBrokers) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topic = topic;
        this.seedBrokers = seedBrokers;
        partitionCache = new HashMap<>();
    }

    /**
     * Consumer constructor
     * Requires a list of seed brokers that will be used for bootstrapping
     * You don't have to specify the whole list as they will be auto-discovered
     * @param topic Topic to work on
     * @param seedBrokers List of seed brokers
     */
    public Consumer(String topic, List<String> seedBrokers) {
        this(new KafkaConsumerFactory(), topic, seedBrokers);
    }

    /**
     * Return list of Partition IDs for the given topic
     * Metadata API
     * @return List of Partitions IDs
     */
    public List<Integer> getAvailablePartitions() {
        TopicConsumer topicConsumer = this.kafkaConsumerFactory.topicConsumer(this.topic, this.seedBrokers);
        return topicConsumer.getPartitions().stream()
                .map(PartitionLeader::getPartitionId)
                .collect(Collectors.toList());
    }

    /**
     * Return a map of latest offsets
     * Fetch/Offet API
     * @return Map of latest offsets
     */
    public Map<Integer, Long> getLatestOffsets() {
        TopicConsumer topicConsumer = this.kafkaConsumerFactory.topicConsumer(this.topic, this.seedBrokers);
        FetchConsumer fetchConsumer = this.kafkaConsumerFactory.fetchConsumer();

        Map<Integer, Long> offsetMap = new HashMap<>();
        topicConsumer.getPartitions().forEach(partitionLeader -> {
            offsetMap.put(
                    partitionLeader.getPartitionId(),
                    fetchConsumer.getOffset(
                            this.topic, partitionLeader.getLeader(), partitionLeader.getPartitionId()
                    )
            );
        });
        return offsetMap;
    }

    private ByteBufferMessageSet getBatch(String topic, int partition, Broker leader, Long offset) {
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer consumer = this.kafkaConsumerFactory.simpleConsumer(
                leader.host(), leader.port(), 100000, 64 * 1024, clientName
        );
        if (null == offset) {
            offset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        }

        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                .addFetch(topic, partition, offset, 100000)
                .build();
        FetchResponse fetchResponse = consumer.fetch(req);
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
        return fetchResponse.messageSet(topic, partition);
    }


    /**
     * Return batch of messages for these partitions and offsets
     * This is the main operational method of this library, this is how you get your messages
     * Offsets are intrinsically attached to their partitions so they always have to be passed together.
     * Its valid to specify less partitions than are actually available, but other way around throws an exception.
     * @TODO: Refactor
     * @param partitionOffsetMap Partitions and their offets to look at
     * @return List of Messages from all partitions
     */
    public List<Message> getBatchFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        List<Message> list = new ArrayList<>();

        // Loop through the provided partition map
        // @TODO: Convert to stream for ability to add parallelism.
        for (Map.Entry<Integer, Long> bit : partitionOffsetMap.entrySet()) {
            int partition = bit.getKey();
            Long offset = bit.getValue();

            Broker leader = getLeaderForPartition(partition);

            // GET BATCH (topic, partition, leader, offset);
            ByteBufferMessageSet messageSet = getBatch(topic, partition, leader, offset);

            for (MessageAndOffset messageAndOffset : messageSet) {
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
        }
        return list;
    }

    private Broker getLeaderForPartition(Integer partition) {
        // If Partition is not in the cache,
        if (!partitionCache.containsKey(partition)) {
            TopicConsumer topicConsumer = this.kafkaConsumerFactory.topicConsumer(this.topic, this.seedBrokers);
            List<PartitionLeader> partitions = topicConsumer.getPartitions();
            // Re-populate the cache
            partitionCache.putAll(
                    partitions.stream()
                            .collect(Collectors.toMap(
                                    PartitionLeader::getPartitionId,
                                    PartitionLeader::getLeader
                            ))
            );
            // Partition cache shouldn't contain more partitions than we get from Kafka
            // You can't remove partitions in Kafka!
            assert partitionCache.size() == partitions.size();
            if (! partitionCache.containsKey(partition)) {
                throw new PartitionException("Specified partition doesn't exist");
            }
        }
        Broker leader = partitionCache.get(partition);
        return leader;
    }

    @Deprecated
    private void getLeaders() {
        TopicConsumer topicConsumer = this.kafkaConsumerFactory.topicConsumer(this.topic, this.seedBrokers);
        List<PartitionLeader> partitions = topicConsumer.getPartitions();
        for (PartitionLeader partition : partitions) {
            this.partitionCache.put(partition.getPartitionId(), partition.getLeader());
        }
    }

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