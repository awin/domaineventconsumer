package com.zanox.application;

import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;
import com.zanox.application.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;

public class Processor {
    private final EventHandlerFactory eventHandlerFactory;
    private EventParser parser;

    public Processor() {
        parser = new EventParser();
        eventHandlerFactory = new EventHandlerFactory();

    }

    public Processor(EventParser parser, EventHandlerFactory handlerFactory) {
        this.parser = parser;
        this.eventHandlerFactory = handlerFactory;
    }

    public void process(byte[] message) {
        parser.handle(message);
    }
}
