package com.zanox.kafka.durable.test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelStream {
    /**
     * A test of Parallel processing framework
     * Imitates Kafka partition consumer which consumes from each partition in parallel
     */
    public ConcurrentLinkedQueue<TestMessage> test(int partitions, int messagesPerPartition, int delay) {
        long start = System.currentTimeMillis();
        ConcurrentLinkedQueue<TestMessage> list = new ConcurrentLinkedQueue<>();

        IntStream.range(0, partitions).parallel().boxed().flatMap(partition -> {
            // Really expensive operation, ie: Kafka partition fetch
            sleep(delay);
            return IntStream.range(1, messagesPerPartition).boxed().map(message -> new TestMessage(partition, message));
        }).forEach(testMessage -> {
            testMessage.processedByThread(Thread.currentThread().getId());
            list.add(testMessage);
        });
        System.out.format(
                "Time taken to process %s partitions (%s ms delay per partition) with %s messages each is %s ms %n",
                partitions, delay, messagesPerPartition, System.currentTimeMillis() - start
        );
        System.out.format(
                "Should have taken (%s ms * %s) = %s ms %n",
                delay, partitions, delay * partitions
        );
        return list;
    }

    public Map<Integer, List<TestMessage>> getPartitionMap(ConcurrentLinkedQueue<TestMessage> list) {
        Map<Integer, List<TestMessage>> a = list.stream().collect(Collectors.groupingBy(TestMessage::getPartition));
        return a;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
