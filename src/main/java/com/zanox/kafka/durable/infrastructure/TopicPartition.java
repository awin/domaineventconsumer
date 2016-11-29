package com.zanox.kafka.durable.infrastructure;

import kafka.cluster.Broker;

public class TopicPartition {
    private Integer id;
    private Broker leader;

    public TopicPartition(Integer id, Broker leader) {
        this.id = id;
        this.leader = leader;
    }

    public Integer getId() {
        return id;
    }

    public Broker getLeader() {
        return leader;
    }
}
