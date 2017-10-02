package com.zanox.kafka.durable.integration

import com.zanox.kafka.durable.Consumer
import com.zanox.kafka.durable.Message
import com.zanox.kafka.durable.EmbeddedKafka
import com.zanox.kafka.durable.EmbeddedKafkaProducer
import spock.lang.Ignore
import spock.lang.Specification

import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream

class IntegrationTest extends Specification {

    EmbeddedKafka embeddedKafka
    EmbeddedKafkaProducer producer

    def setup() {
        embeddedKafka = new EmbeddedKafka()
        embeddedKafka.start()
        println("Running on " + embeddedKafka.getKafkaConnectString() + " Kafka")
        producer = new EmbeddedKafkaProducer(embeddedKafka)
    }

    def cleanup() {
        embeddedKafka.stop()
    }

    def "try running normal Kafka"() {
        setup:
        producer.createTopic("benchmark", 4)
        def consumer = new Consumer("benchmark", [embeddedKafka.getKafkaConnectString()])
        def c = Mock(java.util.function.Consumer)
        def map = [0:0L, 1:0L, 2:0L, 3:0L]

        when:
        IntStream.range(0, 100).parallel().forEach({
            producer.sendMessage("benchmark", "foo1", "bar")
        })
        def offsets = consumer.getLatestOffsets()

        consumer.getBatchFromPartitionOffset(consumer.getEarliestOffsets()).forEach(c)

        then:
        println(offsets)
        100 * c.accept({ Message message ->
            assert new String(message.body) == "bar"
            assert message.offset == map[message.partition] + 1
            map[message.partition] = message.offset;
            return true;
        })
        0 * _
    }

    @Ignore
    def "Try streaming producer"() {
        setup:
        producer.createTopic("benchmark", 4)
        def consumer = new Consumer("benchmark", [embeddedKafka.getKafkaConnectString()])
        def c = Mock(java.util.function.Consumer)
        def map = [0:0L, 1:0L, 2:0L, 3:0L]

        when:
        IntStream.range(0, 100).parallel().forEach({
            producer.sendMessage("benchmark", "foo1", "bar")
        })
        def offsets = consumer.getLatestOffsets()

        consumer.getStreamFromPartitionOffset(consumer.getEarliestOffsets())
                .unordered().limit(10).peek({ m -> print(m)})
                .forEach(c)

        then:
        println(offsets)
        100 * c.accept({ Message message ->
            println(new String(message.body))
            assert new String(message.body) == "bar"
            assert message.offset == map[message.partition] + 1
            map[message.partition] = message.offset;
            return true;
        })
        0 * _
    }

    @Ignore
    def "Try new streaming method"() {
        setup:
        producer.createTopic("benchmark", 4)
        def consumer = new Consumer("benchmark", [embeddedKafka.getKafkaConnectString()])
        def c = Mock(java.util.function.Consumer)
        def map = [0:0L, 1:0L, 2:0L, 3:0L]
        IntStream.range(0, 100).parallel().forEach({ key ->
            producer.sendMessage("benchmark", key.toString(), "bar")
        })
        def offsets = consumer.getLatestOffsets()
        println(offsets)
        println(consumer.getEarliestOffsets())

        when:
        consumer.streamPartitions(consumer.getEarliestOffsets())
                .parallelStream()
                .forEach({ partition ->
            partition.forEach({ messageStream ->
                messageStream.forEach({ message ->
                    System.out.println(new String(message.body))
                    c.accept(message)
                })
            })
        })

        then:
        100 * c.accept({ Message message ->
            println(new String(message.body))
            assert new String(message.body) == "bar"
            assert message.offset == map[message.partition] + 1
            map[message.partition] = message.offset
            return true
        })
        0 * _
    }

}