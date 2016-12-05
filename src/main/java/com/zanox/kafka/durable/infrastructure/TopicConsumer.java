package com.zanox.kafka.durable.infrastructure;

import com.google.common.net.HostAndPort;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class is an abstraction over a Topic
 * encapsulating multiple Partitions, Brokers, and
 * leaders of these partitions.
 *
 * TopicConsumer exists over a specific Topic
 * and cannot know anything about Leaders or Partitions
 * and especially Offsets. It can however look up
 * Partitions and Leaders.
 *
 * Metadata API
 */
public class TopicConsumer {
    private KafkaConsumerFactory kafkaConsumerFactory;
    private final String topic;
    private final List<String> brokers;

    public TopicConsumer(KafkaConsumerFactory kafkaConsumerFactory, String topic, List<String> brokers) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topic = topic;
        this.brokers = brokers;
    }

    /**
     * Get Partitions and their Leaders for Topic
     * @return List of Partitions and their Leaders
     */
    public List<PartitionLeader> getPartitions() {
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        for (String broker : brokers) {
            List<TopicMetadata> metaData = new ArrayList<>();
            try {
                HostAndPort hostAndPort = HostAndPort.fromString(broker);
                SimpleConsumer consumer = this.kafkaConsumerFactory.simpleConsumer(
                        hostAndPort.getHostText(), hostAndPort.getPortOrDefault(9092), 100000, 64 * 1024, "leaderLookup"
                );
                TopicMetadataResponse resp = consumer.send(req);
                consumer.close();
                metaData = resp.topicsMetadata();
            } catch (Exception e) {
                // @TODO: Rewrite this to only throw exception if none of the brokers responded
                e.printStackTrace();
            }

            Optional<List<PartitionMetadata>> f = metaData.stream()
                    .map(TopicMetadata::partitionsMetadata)
                    .filter(list -> !list.isEmpty())
                    .findAny();
            if (!f.isPresent()) {
                throw new PartitionException("Partition list is empty");
            }

            return f.get().stream()
                    .map(partitionMetadata ->
                            new PartitionLeader(partitionMetadata.partitionId(), partitionMetadata.leader()))
                    .collect(Collectors.toList());
        }
        throw new PartitionException("Can't find any partitions");
    }
}
