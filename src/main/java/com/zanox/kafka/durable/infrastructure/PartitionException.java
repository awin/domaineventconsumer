package com.zanox.kafka.durable.infrastructure;

public class PartitionException extends RuntimeException {
    public PartitionException(String s) {
        super(s);
    }
}
