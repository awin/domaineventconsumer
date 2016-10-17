package com.zanox.application.model;

/**
 * @todo refactor make attributes read only
 */
public class MembershipId {
    public final Integer advertiserId;
    public final Integer publisherId;

    public MembershipId(Integer advertiserId, Integer publisherId)
    {
        this.advertiserId = advertiserId;
        this.publisherId = publisherId;
    }
}
