package com.zanox.kafka.durable.infrastructure

import kafka.cluster.BrokerEndPoint
import kafka.javaapi.FetchResponse
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.message.ByteBufferMessageSet
import spock.lang.Specification

class MessageConsumerTest extends Specification {
    def "It can fetch a message list"() {
        setup:
        def consumerFactory = Mock(KafkaConsumerFactory)
        def leader = Mock(BrokerEndPoint)
        def consumer = new MessageConsumer(consumerFactory, leader)

        when:
        consumer.fetch("topic", 1, 42)

        then:
        1 * leader.host() >> "foo"
        1 * leader.port() >> 1337
        1 * consumerFactory.simpleConsumer("foo", 1337, _, _, _) >> {
            def simpleConsumer = Mock(SimpleConsumer)
            1 * simpleConsumer.close()
            1 * simpleConsumer.fetch(_) >> { // @TODO: Add request?
                def fetchResponse = Mock(FetchResponse)
                1 * fetchResponse.hasError() >> false
                1 * fetchResponse.messageSet("topic", 1) >> {
                    def messageSet = Mock(ByteBufferMessageSet)
                    1 * messageSet.sizeInBytes() >> 1
                    return messageSet
                }
                return fetchResponse
            }
            return simpleConsumer
        }
        0 * _
    }
}