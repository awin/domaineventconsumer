package com.zanox.kafka.durable

import com.zanox.kafka.durable.infrastructure.FetchConsumer
import com.zanox.kafka.durable.infrastructure.KafkaConsumerFactory
import com.zanox.kafka.durable.infrastructure.TopicConsumer
import com.zanox.kafka.durable.infrastructure.PartitionLeader
import kafka.cluster.BrokerEndPoint
import spock.lang.Specification

class ConsumerTest extends Specification {
    def "it can be instantiated"() {
        when:
        def consumer = new Consumer("foo", new ArrayList<String>())

        then:
        assert consumer instanceof Consumer
        0 * _
    }

    def "it can get a list of available partitions from Kafka"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def consumer = new Consumer(consumerFactory, "topic", list)

        when:
        def partitions = consumer.getAvailablePartitions()

        then:
        assert partitions == [0, 1]
        1 * consumerFactory.topicConsumer("topic", ["BrokerSeedURL"]) >> {
            def topicConsumer = Mock(TopicConsumer)
            1 * topicConsumer.getPartitions() >> {
                List<PartitionLeader> partitionList = new ArrayList<>()
                partitionList.add(new PartitionLeader(0, Mock(BrokerEndPoint)))
                partitionList.add(new PartitionLeader(1, Mock(BrokerEndPoint)))
                return partitionList
            }
            return topicConsumer
        }
        0 * _
    }

    def "it can fetch latest offsets"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def consumer = new Consumer(consumerFactory, "topic", list)
        def leader = Mock(BrokerEndPoint)

        def pl1 = Mock(PartitionLeader)
        def pl2 = Mock(PartitionLeader)

        when:
        def offsets = consumer.getLatestOffsets()

        then:
        assert offsets[0].value == 2L
        assert offsets[1].value == 42L
        2 * pl1.getPartitionId() >> 0
        1 * pl1.getLeader() >> leader
        2 * pl2.getPartitionId() >> 1
        1 * pl2.getLeader() >> leader

        1 * consumerFactory.topicConsumer("topic", list) >> {
            def topicConsumer = Mock(TopicConsumer)
            1 * topicConsumer.getPartitions() >> {
                List<PartitionLeader> partitions = new ArrayList<>()
                partitions.add(pl1)
                partitions.add(pl2)
                return partitions
            }
            return topicConsumer
        }
        1 * consumerFactory.fetchConsumer() >> {
            def fetchConsumer = Mock(FetchConsumer)
            1 * fetchConsumer.getOffset("topic", leader, 0, Offset.LATEST) >> new Offset(2L)
            1 * fetchConsumer.getOffset("topic", leader, 1, Offset.LATEST) >> new Offset(42L)
            return fetchConsumer
        }
        0 * _
    }
}