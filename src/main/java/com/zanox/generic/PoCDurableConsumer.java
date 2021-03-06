package com.zanox.generic;

import com.zanox.kafka.durable.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PoCDurableConsumer {
    public static void main(String args[]) {
        String topic = args[0];
        List<String> seeds = new ArrayList<>();
        seeds.add(args[1]);

        // Create a consumer for topic with seed Kafka hosts
        Consumer consumer = new Consumer(topic, seeds);

        // List all available partitions with earliest offsets
        Map<Integer, Long> offsetMap = consumer.getEarliestOffsets();
        System.err.println("We start with this offset map: " + offsetMap);

        // If you want to start from latest offsets do:
        System.err.println("This are the latest offsets: " + consumer.getLatestOffsets());

        // Build a statistics map
        Map<Integer, AtomicInteger> countMap = new ConcurrentHashMap<>();
        offsetMap.forEach((key, value) ->
            countMap.put(key, new AtomicInteger(0))
        );

        /*
         * Consumer gives us a List of Partitions we turn that into a parallel stream,
         * so we can consume a partition in a thread. Inside each partition is a message
         * stream which represents one batch of messages. Inside each batch is a message
         *
         * Users of the consumer can easily switch between parallel and serial
         * consumption.
         */
        consumer.streamPartitions(offsetMap).parallelStream().forEach(partition -> {
            partition.forEach(messageBatch -> {
                messageBatch.forEach(message -> {
                    countMap.get(message.partition).incrementAndGet();
                    System.out.format(
                            "Partition: %s Offset: %s Thread: %s %s %n",
                            message.partition, message.offset, Thread.currentThread().getId(), countMap
                    );
                });
            });
        });
    }
}
