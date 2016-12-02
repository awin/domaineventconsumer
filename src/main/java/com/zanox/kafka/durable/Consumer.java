package com.zanox.kafka.durable;

import com.zanox.kafka.durable.infrastructure.FetchConsumer;
import com.zanox.kafka.durable.infrastructure.KafkaConsumerFactory;
import com.zanox.kafka.durable.infrastructure.MessageConsumer;
import com.zanox.kafka.durable.infrastructure.PartitionException;
import com.zanox.kafka.durable.infrastructure.TopicConsumer;
import com.zanox.kafka.durable.infrastructure.PartitionLeader;
import kafka.cluster.Broker;
import kafka.message.MessageAndOffset;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Consumer {
    private static final boolean EXECUTE_MAPS_IN_PARALLEL = false;
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

    private Iterable<MessageAndOffset> getBatch(String topic, int partition, Broker leader, Long offset) {
        MessageConsumer messageConsumer = this.kafkaConsumerFactory.messageConsumer(leader);
        if (null == offset) {
            FetchConsumer fetchConsumer = this.kafkaConsumerFactory.fetchConsumer();
            offset = fetchConsumer.getOffset(topic, leader, partition);
        }
        return messageConsumer.fetch(topic, partition, offset);
    }


    /**
     * Return batch of messages for these partitions and offsets
     * This is the main operational method of this library, this is how you get your messages
     * Offsets are intrinsically attached to their partitions so they always have to be passed together.
     * Its valid to specify less partitions than are actually available, but other way around throws an exception.
     * @TODO: Provide another function to give you infinite parallel stream instead
     * @param partitionOffsetMap Partitions and their offets to look at
     * @return List of Messages from all partitions
     */
    public List<Message> getBatchFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        return getStreamFromPartitionOffset(partitionOffsetMap, true)
                .collect(Collectors.toList());
    }

    /**
     * Get batches of messages as an Infinite stream
     * Note that Offset handling is slightly different here, You are no longer required to keep the offsets yourself
     * simply supply the correct offset on stream start.
     * @TODO: Implement closing streams
     * @param partitionOffsetMap Partitions and their offsets
     * @return Stream of messages
     */
    public Stream<Message> getStreamFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        return getStreamFromPartitionOffset(partitionOffsetMap, false);
    }

    private Stream<Message> getStreamFromPartitionOffset(Map<Integer, Long> partitionOffsetMap, boolean limit) {
        Stream<Map.Entry<Integer, Long>> stream = partitionOffsetMap.entrySet().stream();
        if (EXECUTE_MAPS_IN_PARALLEL) {
            stream = stream.parallel();
        }

        return stream.flatMap(entry -> {
            int partition = entry.getKey();

            Broker leader = getLeaderForPartition(partition);
            final AtomicLong offset = new AtomicLong(entry.getValue());
            return Stream.generate(() -> {
                // Infinite stream
                Iterable<MessageAndOffset> messageSet = getBatch(topic, partition, leader, offset.get());

                // Finite stream
                Stream<Message> finiteStream = StreamSupport.stream(messageSet.spliterator(), false).map(messageAndOffset -> {
                    offset.getAndSet(messageAndOffset.nextOffset());

                    ByteBuffer payload = messageAndOffset.message().payload();

                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);

                    Message message = new Message();
                    message.body = bytes;
                    // @TODO: Can this be refactored so we don't have a nested map() calls? also partition variable
                    message.partition = partition;
                    message.offset = messageAndOffset.nextOffset(); // This is just `offset + 1L`
                    return message;
                });
                if (limit) {
                    // @TODO: I really don't want to walk through the collection again
                    finiteStream.limit(10000);
                }
                return finiteStream;
            }).flatMap(Function.identity());
        });
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
}