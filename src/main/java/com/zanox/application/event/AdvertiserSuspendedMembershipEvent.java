package com.zanox.application.event;

import com.zanox.application.DomainEvent;

public class AdvertiserSuspendedMembershipEvent extends DomainEvent {

    public Integer advertiserId;
    public Integer publisherId;
}
