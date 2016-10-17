package com.zanox.application;

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
        DomainEvent event;

        try {
            event = parser.getEventFromMessage(message);
        } catch (BadMessageException e) {
            System.err.println("Unable to parse message");

            return;
        } catch (UnsupportedEvent e) {
            System.out.println("Unsupported event");

            return;
        }


        try {
            DomainEventHandler eventHandler = eventHandlerFactory.getHandlerByEvent(event);
            eventHandler.handle(event);
        } catch (UnableToHandleEvent e) {
            System.err.println("Unable to parse message");

            return;
        }
    }
}
