package com.zanox.application.eventHandler;

import com.zanox.application.DomainEvent;
import com.zanox.application.DomainEventHandler;
import com.zanox.application.UnableToHandleEvent;
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;

public class AdvertiserAcceptedMembershipApplicationEventHandler implements DomainEventHandler<AdvertiserAcceptedMembershipApplicationEvent>
{
    public void handle(AdvertiserAcceptedMembershipApplicationEvent event) throws UnableToHandleEvent
    {

    }
}
