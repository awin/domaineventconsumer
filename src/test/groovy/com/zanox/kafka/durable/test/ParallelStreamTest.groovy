package groovy.com.zanox.kafka.durable.test

import com.zanox.kafka.durable.ParallelStream
import com.zanox.kafka.durable.TestMessage
import spock.lang.Ignore
import spock.lang.Specification

import java.util.stream.Stream

class ParallelStreamTest extends Specification {
    def "Parallel stream test"() {
        setup:
        def parallel = new ParallelStream()
        when:
        def list = parallel.test(200, 1000, 20);

        then:
        def map = parallel.getPartitionMap(list);
        testPartitionMap(map)
        0 * _
    }

    @Ignore
    def "Parallel test with Infinite streams"() {
        setup:
        def parallel = new ParallelStream();

        when:
        Stream<Stream<TestMessage>> list = parallel.testInfiniteStreams(2, 200);
        Map<Integer, Integer> map = new HashMap<>();
        list.forEach({ testStream ->
            testStream.limit(15).forEach({ testMessage ->
                if (!map.containsKey(testMessage.partition)) {
                    map.put(testMessage.partition, 0);
                }
                int count = map.get(testMessage.partition)
                assert testMessage.message == count + 1
                map.put(testMessage.partition, count + 1)

                System.err.format("Partition: %s, Message: %s from Thread: %s %n", testMessage.partition, testMessage.message, Thread.currentThread().getId());
                System.err.println(map);
                Thread.sleep(100);
            })
        })

        then:
        0 * _
    }

    private static void testPartitionMap(Map<Integer, List<TestMessage>> a) {
        a.forEach({key, value ->
            int count = 0;
            value.forEach({ testMessage ->
                assert key == testMessage.partition;
                assert count < testMessage.message;
                assert testMessage.message - count == 1;
                count++;
            });
        });
    }

}