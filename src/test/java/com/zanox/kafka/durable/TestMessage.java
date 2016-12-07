package com.zanox.kafka.durable;

public class TestMessage {
    private Integer partition;
    public Integer message;
    public long id;

    public TestMessage(Integer partition, Integer message) {
        this.partition = partition;
        this.message = message;
    }

    public void processedByThread(long id) {
        this.id = id;
    }

    public Integer getPartition() {
        return partition;
    }
}
