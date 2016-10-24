package com.zanox.application;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.zanox.demo.event.AdvertiserAcceptedMembershipApplicationEvent;
import com.zanox.demo.event.AdvertiserSuspendedMembershipEvent;
import com.zanox.demo.event.DomainEvent;
import com.zanox.demo.event.PublisherLeftProgrammeEvent;
import com.zanox.demo.eventHandler.AdvertiserAcceptedMembershipApplicationEventHandler;
import com.zanox.demo.eventHandler.AdvertiserSuspendedMembershipEventHandler;
import com.zanox.demo.eventHandler.PublisherLeftProgrammeEventHandler;
import com.zanox.demo.eventHandler.UnableToHandleEvent;

public class Processor {
    private Gson gson;

    public Processor() {
        gson = new Gson();
    }

    public void process(byte[] message) {
        try {
            DomainEvent event = gson.fromJson(new String(message), DomainEvent.class);
            String eventName = event.eventName;

            switch (eventName) {
                case "AdvertiserAcceptedMembershipApplicationEvent":
                    AdvertiserAcceptedMembershipApplicationEvent specificEvent = gson.fromJson(
                            new String(message),
                            AdvertiserAcceptedMembershipApplicationEvent.class
                    );
                    AdvertiserAcceptedMembershipApplicationEventHandler handler = new AdvertiserAcceptedMembershipApplicationEventHandler();
                    handler.handle(specificEvent);
                    break;
                case "AdvertiserSuspendedMembershipEvent":
                    AdvertiserSuspendedMembershipEvent suspended = gson.fromJson(
                            new String(message),
                            AdvertiserSuspendedMembershipEvent.class
                    );
                    AdvertiserSuspendedMembershipEventHandler suspendHandler = new AdvertiserSuspendedMembershipEventHandler();
                    suspendHandler.handle(suspended);
                    break;
                case "PublisherLeftProgrammeEvent":
                    PublisherLeftProgrammeEvent left = gson.fromJson(
                            new String(message),
                            PublisherLeftProgrammeEvent.class
                    );
                    PublisherLeftProgrammeEventHandler leftHandler = new PublisherLeftProgrammeEventHandler();
                    leftHandler.handle(left);
                    break;
            }
        } catch (JsonParseException | UnableToHandleEvent e) {
            e.printStackTrace();
        }
    }
}
