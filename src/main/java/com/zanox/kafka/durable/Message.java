package com.zanox.kafka.durable;

public class Message {
    public byte[] body;
    public int partition;

    /**
     * Offset of the next message
     */
    public long offset;
}