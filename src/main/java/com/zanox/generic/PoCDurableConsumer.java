package com.zanox.generic;

import com.zanox.kafka.durable.Consumer;
import com.zanox.kafka.durable.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Map<Integer, AtomicInteger> countMap = new HashMap<>();
        offsetMap.forEach((key, value) ->
            countMap.put(key, new AtomicInteger(0))
        );

        // Build up a list of batch suppliers per partition
        List<Supplier<Stream<Message>>> supplierList = offsetMap.entrySet().stream().map(entry ->
            consumer.getBatchSupplierForPartitionAndOffset(entry.getKey(), entry.getValue())
        ).collect(Collectors.toList());

        // Create a parallel stream from suppliers - Ensures we process partitions
        supplierList.parallelStream().forEach(supplier -> {
            // Create an infinite stream from each supplier
            Stream.generate(() -> supplier).forEach(batchSupplier -> {
                // Process each batch separately
                batchSupplier.get().forEach(message -> {
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
