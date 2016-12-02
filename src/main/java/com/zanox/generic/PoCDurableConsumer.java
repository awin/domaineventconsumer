package com.zanox.generic;

import com.zanox.kafka.durable.Consumer;
import com.zanox.kafka.durable.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PoCDurableConsumer {
    public static void main(String args[]) {
        String topic = args[0];
        List<String> seeds = new ArrayList<>();
        seeds.add(args[1]);

        Consumer example = new Consumer(topic, seeds);

        // All available partitions
        // We can use this to look up which partitions are we responsible for.
        List<Integer> partitions = example.getAvailablePartitions();
        Map<Integer, Long> offsetMap = new HashMap<>();
        for (Integer partition : partitions) {
            offsetMap.put(partition, null); // BEGINNING specified as `null`
        }
        System.err.println("We start with this offset map: " + offsetMap);

        // If you want to start from latest offsets do:
        offsetMap = example.getLatestOffsets();
        System.err.println("This are the latest offsets: " + offsetMap);

        example.getStreamFromPartitionOffset(offsetMap).forEach(message -> {
            System.out.println(new String(message.body));
            System.out.format("Persist offsets: %s:%s %n", message.partition, message.offset);
        });
    }
}
