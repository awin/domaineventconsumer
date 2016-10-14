package com.zanox.application;

import com.zanox.application.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;

public class EventHandlerFactory {
    public DomainEventHandler getHandlerByEvent(DomainEvent event) {
        switch (event.eventName) {
            case "AdvertiserAcceptedMembershipApplicationEventHandler":
                return new AdvertiserAcceptedMembershipApplicationEventHandler();
        }

        return null;
    }
}
