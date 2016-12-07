package com.zanox.kafka.durable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This class is here for testing assumptions about how Parallel Streams in Java8 work. This isn't a class you should
 * be including in any way
 */
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

    /**
     * Test of infinite streams. This actually doesn't work, as it results in an infinite loop when trying to limit the
     * stream size or close it. The test for this method is disabled
     * @param partitions Number of partitions to simulate
     * @param delay Delay for each batch
     * @return Stream of Streams of TestMessages
     */
    public Stream<Stream<TestMessage>> testInfiniteStreams(int partitions, long delay) {
        // Partition stream
        return IntStream.range(0, partitions).unordered().parallel().boxed().map(partition -> {
            sleep(delay);
            AtomicInteger offset = new AtomicInteger(0);
            // Infinite Stream - Kafka partition
            return Stream.generate(() -> {
                System.out.print(".");
                // Generate 10 messages
                return IntStream.range(0, 10).boxed().map(y ->
                        new TestMessage(partition, offset.incrementAndGet())
                );
            }).flatMap(Function.identity());
        });
    }

    /**
     * Helper method to get a map of partitions
     * @param list queue
     * @return map of partitions
     */
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
