package com.zanox.kafka.durable.infrastructure;

import kafka.cluster.Broker;

/**
 * This encapsulates a Leader for a Partition
 * It might have the knowledge of offsets.
 */
public class PartitionLeader {
    private Integer partitionId;
    private Broker leader;

    public PartitionLeader(Integer partitionId, Broker leader) {
        this.partitionId = partitionId;
        this.leader = leader;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public Broker getLeader() {
        return leader;
    }
}
