package com.zanox.application

import kafka.consumer.KafkaStream
import spock.lang.Specification


class ConsumerFactoryTest extends Specification {
    def "It should create a test consumer"() {
        setup:
        def consumerFactory = new ConsumerFactory()

        when:
        def consumer = consumerFactory.create(Mock(KafkaStream), 1)

        then:
        consumer instanceof Consumer
    }
}
