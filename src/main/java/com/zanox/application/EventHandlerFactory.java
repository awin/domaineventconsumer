package com.zanox.application;

import com.zanox.application.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;

public class EventHandlerFactory {
    public DomainEventHandler getHandlerByEvent(DomainEvent event) {
        switch (event.eventName) {
            case "AdvertiserAcceptedMembershipApplicationEventHandler":
                return this.createAdvertiserAcceptedMembershipApplicationEventHandler();
        }

        return null;
    }

    private DomainEventHandler createAdvertiserAcceptedMembershipApplicationEventHandler() {
        RedisMapper mapper = new RedisMapper();
        DomainEventHandler handler = new AdvertiserAcceptedMembershipApplicationEventHandler(mapper);
        return handler;
    }
}
