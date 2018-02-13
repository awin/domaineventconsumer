package com.zanox.kafka.durable;

import com.zanox.kafka.durable.infrastructure.FetchConsumer;
import com.zanox.kafka.durable.infrastructure.KafkaConsumerFactory;
import com.zanox.kafka.durable.infrastructure.MessageConsumer;
import com.zanox.kafka.durable.infrastructure.TopicConsumer;
import com.zanox.kafka.durable.infrastructure.PartitionLeader;
import kafka.cluster.BrokerEndPoint;
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
    private final Map<Integer, BrokerEndPoint> partitionCache;
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
     * Get Infinite stream of messages per partition
     * @param partition Partition ID
     * @param offsetLong Offset to start from
     * @return Infinite Stream of messages
     */
    private Supplier<Stream<Message>> getBatchSupplierForPartitionAndOffset(int partition, Long offsetLong) {
        BrokerEndPoint leader = getLeaderForPartition(partition);
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

    private BrokerEndPoint getLeaderForPartition(Integer partition) {
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
        BrokerEndPoint leader = partitionCache.get(partition);
        return leader;
    }

    public List<Stream<Stream<Message>>> streamPartitions(Map<Integer, Long> offsetMap) {
        return offsetMap.entrySet().stream().map(entry ->
            this.getBatchSupplierForPartitionAndOffset(entry.getKey(), entry.getValue())
        ).map(Stream::generate).collect(Collectors.toList());
    }
}