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
        Map<Integer, Long> OffsetMap = new HashMap<>();
        for (Integer partition : partitions) {
            OffsetMap.put(partition, null); // BEGINNING specified as `null`
        }
        System.out.println("We start with this offset map: "+OffsetMap);
        while (true) {
            List<Message> batch = example.getBatchFromPartitionOffset(OffsetMap);
            for (Message message : batch) {
                //System.out.print(".");
                // Record the new offset (Possibly wait for the last message in the batch)
                OffsetMap.put(message.partition, message.offset);

            }
            // Persist OffsetMap.
            System.out.println(OffsetMap);
        }
    }
}
