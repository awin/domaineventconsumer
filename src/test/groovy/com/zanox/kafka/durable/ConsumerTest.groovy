package groovy.com.zanox.kafka.durable

import com.zanox.kafka.durable.Consumer
import com.zanox.kafka.durable.infrastructure.FetchConsumer
import com.zanox.kafka.durable.infrastructure.KafkaConsumerFactory
import com.zanox.kafka.durable.infrastructure.TopicConsumer
import com.zanox.kafka.durable.infrastructure.PartitionLeader
import kafka.cluster.Broker
import kafka.javaapi.FetchResponse
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.message.Message
import kafka.message.MessageAndOffset
import spock.lang.Specification

import java.nio.ByteBuffer

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
                partitionList.add(new PartitionLeader(0, Mock(Broker)))
                partitionList.add(new PartitionLeader(1, Mock(Broker)))
                return partitionList;
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
        def leader = Mock(Broker)

        def pl1 = Mock(PartitionLeader)
        def pl2 = Mock(PartitionLeader)

        when:
        def offsets = consumer.getLatestOffsets()

        then:
        assert offsets == [0: 2L, 1: 42L]
        2 * pl1.getPartitionId() >> 0
        1 * pl1.getLeader() >> leader
        2 * pl2.getPartitionId() >> 1
        1 * pl2.getLeader() >> leader

        1 * consumerFactory.topicConsumer("topic", list) >> {
            def topicConsumer = Mock(TopicConsumer)
            1 * topicConsumer.getPartitions() >> {
                List<PartitionLeader> partitions = new ArrayList<>();
                partitions.add(pl1)
                partitions.add(pl2)
                return partitions
            }
            return topicConsumer
        }
        1 * consumerFactory.fetchConsumer() >> {
            def fetchConsumer = Mock(FetchConsumer)
            1 * fetchConsumer.getOffset("topic", leader, 0) >> 2L
            1 * fetchConsumer.getOffset("topic", leader, 1) >> 42L
            return fetchConsumer
        }
        0 * _
    }

    def "It can get a batch of messages"() {
        setup:
        List<String> list = Collections.singletonList("BrokerSeedURL");
        def consumerFactory = Mock(KafkaConsumerFactory)
        def consumer = new Consumer(consumerFactory, "topic", list);
        Map<Integer, Long> offsetMap = new HashMap<>();
        offsetMap.put(1, 42L);

        when:
        def batch = consumer.getBatchFromPartitionOffset(offsetMap)

        then:
        assert batch.size() == 1
        with(batch.first()) {
            assert body == "body".bytes
            assert partition == 1
            assert offset == 43L
        }
        1 * consumerFactory.topicConsumer("topic", list) >> {
            def topicConsumer = Mock(TopicConsumer)
            def partitionLeader = Mock(PartitionLeader)
            1 * partitionLeader.getPartitionId() >> 1
            1 * partitionLeader.getLeader() >> {
                def leader = Mock(Broker)
                1 * leader.host() >> "foo"
                1 * leader.port() >> 1337
                return leader
            }
            1 * topicConsumer.getPartitions() >> Collections.singletonList(partitionLeader)
            return topicConsumer
        }
        1 * consumerFactory.simpleConsumer("foo", 1337, _, _, _) >> {
            def simpleConsumer = Mock(SimpleConsumer)
            1 * simpleConsumer.close()
            1 * simpleConsumer.fetch(_) >> {
                def fetchResponse = Mock(FetchResponse)
                1 * fetchResponse.hasError() >> false
                1 * fetchResponse.messageSet("topic", 1) >> {
                    def messageSet = Mock(ByteBufferMessageSet)
                    1 * messageSet.iterator() >> {
                        def iterator = Mock(Iterator)
                        2 * iterator.hasNext() >>> [true, false]
                        1 * iterator.next() >> {
                            def message = Mock(Message)
                            1 * message.payload() >> ByteBuffer.wrap("body".bytes)
                            return new MessageAndOffset(message, 42L)
                        }
                        return iterator
                    }
                    return messageSet
                }
                return fetchResponse
            }
            return simpleConsumer
        }
        0 * _
    }
}