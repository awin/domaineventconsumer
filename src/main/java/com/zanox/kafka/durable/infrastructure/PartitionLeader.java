package com.zanox.kafka.durable.infrastructure;

import kafka.cluster.BrokerEndPoint;

/**
 * This encapsulates a Leader for a Partition
 * It might have the knowledge of offsets.
 */
public class PartitionLeader {
    private Integer partitionId;
    private BrokerEndPoint leader;

    public PartitionLeader(Integer partitionId, BrokerEndPoint leader) {
        this.partitionId = partitionId;
        this.leader = leader;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public BrokerEndPoint getLeader() {
        return leader;
    }
}
