package com.zanox.kafka.durable.infrastructure

import com.zanox.kafka.durable.PartitionException
import kafka.javaapi.TopicMetadata
import kafka.javaapi.TopicMetadataResponse
import kafka.javaapi.consumer.SimpleConsumer
import spock.lang.Specification

class TopicConsumerTest extends Specification {
    def "it throws exception when list of partitions is empty"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def topicConsumer = new TopicConsumer(consumerFactory, "topic", list)

        when:
        topicConsumer.getPartitions()

        then:
        1 * consumerFactory.simpleConsumer("BrokerSeedURL", 9092, _, _, "leaderLookup") >> {
            def kafkaSimpleConsumer = Mock(SimpleConsumer)
            1 * kafkaSimpleConsumer.close()
            1 * kafkaSimpleConsumer.send(_) >> {
                def response = Mock(TopicMetadataResponse)
                1 * response.topicsMetadata() >> new ArrayList<TopicMetadata>()
                return response
            }()
            return kafkaSimpleConsumer
        }
        0 * _
        thrown PartitionException;
    }
}