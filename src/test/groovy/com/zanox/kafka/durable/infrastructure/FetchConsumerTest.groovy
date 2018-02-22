package com.zanox.kafka.durable.infrastructure

import com.zanox.kafka.durable.Offset
import kafka.cluster.BrokerEndPoint
import kafka.javaapi.OffsetRequest
import kafka.javaapi.OffsetResponse
import kafka.javaapi.consumer.SimpleConsumer
import spock.lang.Specification

class FetchConsumerTest extends Specification {
    def "it can get an offset"() {
        setup:
        def factory = Mock(KafkaConsumerFactory)
        def fetchConsumer = new FetchConsumer(factory)
        def broker = Mock(BrokerEndPoint)

        when:
        def offset = fetchConsumer.getOffset("topic", broker, 42, Offset.LATEST)


        then:
        assert offset.value == 2L
        1 * broker.host() >> "brokerHost"
        1 * broker.port() >> 1337
        1 * factory.simpleConsumer("brokerHost", 1337, _, _, _) >> {
            def consumer = Mock(SimpleConsumer)
            1 * consumer.getOffsetsBefore(_ as OffsetRequest) >> {
                def offsetResponse = Mock(OffsetResponse)
                1 * offsetResponse.hasError() >> false
                1 * offsetResponse.offsets("topic", 42) >> [2L]
                return offsetResponse
            }
            return consumer
        }
        0 * _
    }
}