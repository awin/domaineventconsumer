package com.zanox.kafka.durable;

public class PartitionException extends RuntimeException {
    public PartitionException(String s) {
        super(s);
    }
}
