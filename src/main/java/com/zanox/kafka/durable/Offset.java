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

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Offset offset = (Offset) o;

        return value == offset.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
