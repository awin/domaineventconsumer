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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Consumer {
    private final Map<Integer, Broker> partitionCache;
    private final List<String> seedBrokers;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private String topic;
    private Map<Integer, Long> offsetCache = new HashMap<>();

    /**
     * Constructor with Injectable ConsumerFactory
     *
     * @param kafkaConsumerFactory Kafka Consumer factory
     * @param topic                Topic to work on
     * @param seedBrokers          List of seed brokers
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
     *
     * @param topic       Topic to work on
     * @param seedBrokers List of seed brokers
     */
    public Consumer(String topic, List<String> seedBrokers) {
        this(new KafkaConsumerFactory(), topic, seedBrokers);
    }

    /**
     * Return list of Partition IDs for the given topic
     * Metadata API
     *
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
     *
     * @return Map of latest offsets
     */
    public Map<Integer, Long> getLatestOffsets() {
        return getOffsets(FetchConsumer.LATEST);
    }

    /**
     * Return earliest offsets
     * @return Map of earliest offsets
     */
    public Map<Integer, Long> getEarliestOffsets() {
        return getOffsets(FetchConsumer.EARLIEST);
    }

    private Map<Integer, Long> getOffsets(long whichTime) {
        TopicConsumer topicConsumer = this.kafkaConsumerFactory.topicConsumer(this.topic, this.seedBrokers);
        FetchConsumer fetchConsumer = this.kafkaConsumerFactory.fetchConsumer();

        Map<Integer, Long> offsetMap = new HashMap<>();
        topicConsumer.getPartitions().forEach(partitionLeader -> {
            offsetMap.put(
                    partitionLeader.getPartitionId(),
                    fetchConsumer.getOffset(
                            this.topic, partitionLeader.getLeader(), partitionLeader.getPartitionId(), whichTime
                    )
            );
        });
        return offsetMap;
    }

    /**
     * Return a List of messages for these partitions and offsets
     * Offsets are intrinsically attached to their partitions so they always have to be passed together.
     * Its valid to specify less partitions than are actually available, but other way around throws an exception.
     * Note: Execution is single-threaded and serial
     *
     * @param partitionOffsetMap Partitions and their offsets to consume
     * @return List of Messages from all selected partitions
     */
    @Deprecated
    public List<Message> getBatchFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        return getStreamFromPartitionOffset(partitionOffsetMap).collect(Collectors.toList());
    }

    /**
     * Get batches of messages as Finite streams
     * Note: Execution is single-threaded and serial
     *
     * @TODO: Implement closing streams
     * @param partitionOffsetMap Partitions and their offsets
     * @return Stream of Streams (of messages)
     */
    public Stream<Message> getStreamFromPartitionOffset(Map<Integer, Long> partitionOffsetMap) {
        Stream<Map.Entry<Integer, Long>> stream = partitionOffsetMap.entrySet().stream();
        return stream.flatMap(entry ->
            getBatchSupplierForPartitionAndOffset(entry.getKey(), entry.getValue()).get()
        );
    }

    /**
     * Get Infinite stream of messages per partition
     * @param partition Partition ID
     * @param offsetLong Offset to start from
     * @return Infinite Stream of messages
     */
    public Supplier<Stream<Message>> getBatchSupplierForPartitionAndOffset(int partition, Long offsetLong) {
        Broker leader = getLeaderForPartition(partition);
        MessageConsumer messageConsumer = this.kafkaConsumerFactory.messageConsumer(leader);

        // It doesn't really need to be atomic, as its only accessed in this thread
        final AtomicLong offset = new AtomicLong(getOffsetIfNull(partition, offsetLong));

        return () -> getFiniteStreamForPartitionAndOffset(messageConsumer, partition, offset);
    }

    private Stream<Message> getFiniteStreamForPartitionAndOffset(MessageConsumer messageConsumer, int partition, AtomicLong offset) {
        Iterable<MessageAndOffset> messageSet = messageConsumer.fetch(topic, partition, offset.get());
        // Finite stream
        return StreamSupport.stream(messageSet.spliterator(), false).map(messageAndOffset -> {
            offset.getAndSet(messageAndOffset.nextOffset());

            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);

            Message message = new Message();
            message.body = bytes;
            message.partition = partition;
            message.offset = messageAndOffset.nextOffset(); // This is just `offset + 1L`
            return message;
        });
    }


    private long getOffsetIfNull(int partition, Long offset) {
        if (null != offset) {
            return offset;
        }

        if (! offsetCache.containsKey(partition)) {
            offsetCache = getEarliestOffsets();
        }
        return offsetCache.get(partition);
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