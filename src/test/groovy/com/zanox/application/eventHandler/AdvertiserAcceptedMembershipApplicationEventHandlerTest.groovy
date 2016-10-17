package com.zanox.application.eventHandler

import com.zanox.application.RedisMapper
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent
import spock.lang.Specification

class AdvertiserAcceptedMembershipEventHandlerTest extends Specification {
    def "It persists a message"() {
        setup:
        def mapper = Mock(RedisMapper)
        AdvertiserAcceptedMembershipApplicationEventHandler handler = new AdvertiserAcceptedMembershipApplicationEventHandler(mapper)
        def event = Mock(AdvertiserAcceptedMembershipApplicationEvent)
        event.publisherId = 1337
        event.advertiserId = 42

        when:
        handler.handle(event)

        then:
        1 * mapper.persist("42-1337", "accepted")
        //1 * event.advertiserId
        //1 * event.publisherId
    }
}