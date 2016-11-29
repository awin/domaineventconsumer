package groovy.com.zanox.kafka.durable

import com.zanox.kafka.durable.Consumer
import com.zanox.kafka.durable.KafkaConsumerFactory
import com.zanox.kafka.durable.infrastructure.TopicConsumer
import com.zanox.kafka.durable.infrastructure.PartitionLeader
import kafka.cluster.Broker
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
        1 * consumerFactory.createConsumerForTopic("topic", ["BrokerSeedURL"]) >> {
            def topicConsumer = Mock(TopicConsumer)
            1 * topicConsumer.getPartitions() >> {
                List<PartitionLeader> partitionList = new ArrayList<>()
                partitionList.add(new PartitionLeader(0, Mock(Broker)))
                partitionList.add(new PartitionLeader(1, Mock(Broker)))
                return partitionList;
            }
            return topicConsumer
        }
        0 * _
    }
}