package com.zanox.kafka.durable;

public class Message {
    public byte[] body;
    public int partition;
    public long offset;
}