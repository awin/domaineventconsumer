package com.zanox.kafka.durable;

public class Offset {
    /**
     * Kafka magic numbers:
     * val kafka.api.OffsetRequest.EarliestTime: Long = -1
     * val kafka.api.OffsetRequest.LatestTime: Long = -2
     * Yes, they are wrong way around
     */
    public static final Offset EARLIEST = new Offset(-2);
    public static final Offset LATEST = new Offset(-1);
    public final long value;

    public Offset(long time) {
        value = time;
    }
}
