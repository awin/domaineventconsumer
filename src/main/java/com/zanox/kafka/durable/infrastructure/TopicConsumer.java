package com.zanox.kafka.durable.infrastructure;

import com.zanox.kafka.durable.KafkaConsumerFactory;
import com.zanox.kafka.durable.PartitionException;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TopicConsumer {
    private KafkaConsumerFactory kafkaConsumerFactory;
    private final String topic;
    private final List<String> brokers;

    public TopicConsumer(String topic, List<String> brokers) {
        this(new KafkaConsumerFactory(), topic, brokers);
    }

    public TopicConsumer(KafkaConsumerFactory kafkaConsumerFactory, String topic, List<String> brokers) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topic = topic;
        this.brokers = brokers;
    }

    public List<TopicPartition> getPartitions() {
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        for (String broker : brokers) {
            List<TopicMetadata> metaData = new ArrayList<>();
            try {
                SimpleConsumer consumer = this.kafkaConsumerFactory.createSimpleConsumer(
                        broker, 9092, 100000, 64 * 1024, "leaderLookup"
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
                    .filter(List::isEmpty)
                    .findAny();
            if (!f.isPresent()) {
                throw new PartitionException("Partition list is empty");
            }

            List<TopicPartition> results = new ArrayList<>();
            f.get().forEach(partitionMetadata -> {
                results.add(new TopicPartition(
                        partitionMetadata.partitionId(),
                        partitionMetadata.leader()
                ));
            });
            return results;
        }
        throw new PartitionException("Can't find any partitions");
    }
}
