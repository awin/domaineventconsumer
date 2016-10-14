package com.zanox.application;

public class Processor {
    private EventParser parser;

    public Processor() {
        parser = new EventParser();
    }

    public Processor(EventParser parser) {
        this.parser = parser;
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

        EventHandlerFactory eventHandlerFactory = new EventHandlerFactory();

        try {
            DomainEventHandler eventHandler = eventHandlerFactory.getHandlerByEvent(event);
            eventHandler.handle(event);
        } catch (UnableToHandleEvent e) {
            System.err.println("Unable to parse message");

            return;
        }
    }
}
