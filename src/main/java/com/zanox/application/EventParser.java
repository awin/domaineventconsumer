package com.zanox.application;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.zanox.application.infrastructure.Parser;
import com.zanox.application.model.Membership;
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;
import kafka.utils.Json;

import java.lang.reflect.Type;

public class EventParser implements Parser<Membership> {

    private Gson gson;

    public EventParser() {
        gson = new Gson();
    }

    @Override
    public Membership parse(byte[] bytes) throws BadMessageException {
        Membership dto = null;
        try {
            dto = gson.fromJson(new String(bytes), Membership.class);
        } catch (JsonParseException e) {
            throw new BadMessageException(e);
        }
        return dto;
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
