package com.zanox.application.event;

import com.zanox.application.DomainEvent;

public class AdvertiserAcceptedMembershipApplicationEvent extends DomainEvent {
    public int advertiserId = 0;
    public int publisherId = 0;
}
