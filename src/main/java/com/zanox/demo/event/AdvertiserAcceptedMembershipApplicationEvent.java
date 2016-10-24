package com.zanox.demo.event;

import com.zanox.demo.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;
import com.zanox.demo.eventHandler.UnableToHandleEvent;

public class AdvertiserAcceptedMembershipApplicationEvent extends DomainEvent {
    public Data data;

    public void visit(AdvertiserAcceptedMembershipApplicationEventHandler handler) throws UnableToHandleEvent {
        handler.handle(this);
    }
}