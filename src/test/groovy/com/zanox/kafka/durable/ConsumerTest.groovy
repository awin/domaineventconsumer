package groovy.com.zanox.kafka.durable

import com.zanox.kafka.durable.Consumer
import com.zanox.kafka.durable.KafkaConsumerFactory
import com.zanox.kafka.durable.PartitionException
import kafka.cluster.Broker
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadata
import kafka.javaapi.TopicMetadataResponse
import kafka.javaapi.consumer.SimpleConsumer
import spock.lang.Specification

class ConsumerTest extends Specification {
    def "it can be instantiated"() {
        when:
        def consumer = new Consumer("foo", new ArrayList<String>())

        then:
        assert consumer instanceof Consumer
        0 * _
    }

    def "it throws exception when list of partitions is empty"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def consumer = new Consumer(consumerFactory, "foo", list)

        when:
        consumer.getAvailablePartitions()

        then:
        // @TODO: Refactor to push Kafka specific stuff lower down
        1 * consumerFactory.createSimpleConsumer("BrokerSeedURL", 9092, _, _, "leaderLookup") >> {
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

    def "it can get a list of available partitions from Kafka"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def consumer = new Consumer(consumerFactory, "foo", list)

        when:
        def partitions = consumer.getAvailablePartitions()

        then:
        assert partitions == [0, 1]
        1 * consumerFactory.createSimpleConsumer("BrokerSeedURL", 9092, _, _, "leaderLookup") >> {
            def kafkaSimpleConsumer = Mock(SimpleConsumer)
            1 * kafkaSimpleConsumer.close()
            1 * kafkaSimpleConsumer.send(_) >> {
                def response = Mock(TopicMetadataResponse)
                1 * response.topicsMetadata() >> Collections.singletonList({
                    TopicMetadata pm = Mock(TopicMetadata);
                    1 * pm.partitionsMetadata() >> {
                        List<PartitionMetadata> partitionList = new ArrayList<>();
                        def f = { id ->
                            def p = Mock(PartitionMetadata);
                            1 * p.partitionId() >> id
                            1 * p.leader() >> Mock(Broker)
                            return p
                        }
                        partitionList.add(f(0))
                        partitionList.add(f(1))
                        return partitionList
                    }()
                    return pm
                }())
                return response
            }()
            return kafkaSimpleConsumer
        }
        0 * _
    }
}