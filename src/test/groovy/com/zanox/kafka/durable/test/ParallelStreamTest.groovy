package groovy.com.zanox.kafka.durable.test

import com.zanox.kafka.durable.test.ParallelStream
import com.zanox.kafka.durable.test.TestMessage
import spock.lang.Specification

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

    public void testPartitionMap(Map<Integer, List<TestMessage>> a) {
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