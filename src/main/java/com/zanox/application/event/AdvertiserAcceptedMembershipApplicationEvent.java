package com.zanox.application.event;

import com.zanox.application.DomainEvent;

public class AdvertiserAcceptedMembershipApplicationEvent extends DomainEvent {
    int advertiserId;
    int publisherId;
}
