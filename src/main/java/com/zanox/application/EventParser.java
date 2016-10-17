package com.zanox.application;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;

import java.lang.reflect.Type;

public class EventParser{

    private Gson gson;

    public EventParser() {
        gson = new Gson();
    }

    public Type getEventClassFromEventName(String eventName) throws DomainEventNotFoundException {
        switch (eventName) {
            case "AdvertiserAcceptedMembershipApplicationEvent":
                return AdvertiserAcceptedMembershipApplicationEvent.class;

        }

        throw new DomainEventNotFoundException(eventName);
    }

    public DomainEvent getEventFromMessage(byte[] message) throws BadMessageException, UnsupportedEvent {
        String string = new String(message);

        DomainEvent genericEvent;

        try {
            genericEvent = gson.fromJson(new String(message), DomainEvent.class);
        } catch (JsonParseException e) {
            throw new BadMessageException();
        }

        Type eventClass;
        try {
            eventClass = getEventClassFromEventName(
                genericEvent.eventName
            );
        } catch (DomainEventNotFoundException e) {
            throw new UnsupportedEvent();
        }

        return gson.fromJson(
            string,
            eventClass
        );
    }
}
