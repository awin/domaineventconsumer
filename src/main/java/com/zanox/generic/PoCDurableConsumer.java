package com.zanox.generic;

import com.zanox.kafka.durable.Consumer;
import com.zanox.kafka.durable.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PoCDurableConsumer {
    public static void main(String args[]) {
        String topic = args[0];
        List<String> seeds = new ArrayList<>();
        seeds.add(args[1]);

        Consumer example = new Consumer(topic, seeds);

        // All available partitions and offsets
        Map<Integer, Long> offsetMap = example.getEarliestOffsets();
        System.err.println("We start with this offset map: " + offsetMap);

        // If you want to start from latest offsets do:
        //offsetMap = example.getLatestOffsets();
        System.err.println("This are the latest offsets: " + example.getLatestOffsets());

        Stream<Message> stream = example.getStreamFromPartitionOffset(offsetMap);
        stream.forEach(message -> {
            //System.out.println(new String(message.body));
            System.out.format("Partition: %s Offset: %s Thread: %s %n", message.partition, message.offset, Thread.currentThread().getId());
        });
    }
}
