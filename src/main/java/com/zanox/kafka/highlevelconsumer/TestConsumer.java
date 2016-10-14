package com.zanox.kafka.highlevelconsumer;

import com.zanox.application.infrastructure.Mapper;
import com.zanox.application.infrastructure.MembershipMapper;
import com.zanox.application.infrastructure.MembershipParser;
import com.zanox.application.infrastructure.Parser;
import com.zanox.application.model.Membership;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import redis.clients.jedis.Jedis;

public class TestConsumer implements Consumer {

    private final KafkaStream stream;
    private final int threadNumber;
    private Jedis jedis;
    private Parser<Membership> parser;
    private Mapper<Membership> mapper;

    public TestConsumer(KafkaStream<byte[], byte[]> stream, int threadNumber) {
        System.out.println("stream: " + stream);
        this.stream = stream;
        this.threadNumber = threadNumber;

        parser = new MembershipParser();
        mapper = new MembershipMapper();
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            byte[] message = it.next().message();

            String output = "Thread " + threadNumber + ": " + new String(message);
            System.out.println(output);

            Membership membership = parser.parse(message);
            mapper.persist(membership);
        }

        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
