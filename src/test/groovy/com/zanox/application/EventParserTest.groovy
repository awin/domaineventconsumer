package com.zanox.application

import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent
import spock.lang.Specification

class MembershipParserTest extends Specification {
    def "It creates correct event"() {
        setup:
        def parser = new EventParser()

        when:
        def type = parser.getEventClassFromEventName("AdvertiserAcceptedMembershipApplicationEvent")

        then:
        assert type == AdvertiserAcceptedMembershipApplicationEvent.class
    }

    def "it rejects non json"() {
        setup:
        def parser = new EventParser()

        when:
        parser.getEventFromMessage("not JSON".bytes)

        then:
        thrown BadMessageException
    }

    def "It parses affiliateId"() {
        setup:
        def parser = new EventParser();

        when:
        def event = parser.getEventFromMessage("{eventName:'AdvertiserAcceptedMembershipApplicationEvent', publisherId:42, advertiserId:1337}".bytes)
        def specificEvent = event as AdvertiserAcceptedMembershipApplicationEvent

        then:
        assert event instanceof DomainEvent
        assert specificEvent.publisherId == 42
        assert specificEvent.advertiserId == 1337
    }
}