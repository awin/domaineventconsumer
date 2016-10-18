package com.zanox.application;

import com.zanox.application.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;
import com.zanox.application.eventHandler.AdvertiserSuspendedMembershipEventHandler;
import com.zanox.application.persistence.MembershipRepository;

public class EventHandlerFactory {
    public DomainEventHandler getHandlerByEvent(DomainEvent event) {
        switch (event.eventName) {
            case "AdvertiserAcceptedMembershipApplicationEventHandler":
                return this.createAdvertiserAcceptedMembershipApplicationEventHandler();
            case "AdvertiserSuspendedMembershipEventHandler":
                return this.createAdvertiserSuspendedMembershipEventHandler();
        }

        return null;
    }

    private DomainEventHandler createAdvertiserAcceptedMembershipApplicationEventHandler() {
        return new AdvertiserAcceptedMembershipApplicationEventHandler();
    }

    private DomainEventHandler createAdvertiserSuspendedMembershipEventHandler() {
        DomainEventHandler handler = new AdvertiserSuspendedMembershipEventHandler();
        return handler;
    }
}
