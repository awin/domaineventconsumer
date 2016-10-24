package com.zanox.demo.eventHandler

import com.zanox.demo.event.AdvertiserAcceptedMembershipApplicationEvent
import com.zanox.demo.model.Membership
import com.zanox.demo.model.MembershipId
import com.zanox.application.persistence.MembershipRepository
import com.zanox.demo.event.Data
import spock.lang.Specification

class AdvertiserAcceptedMembershipEventHandlerTest extends Specification {
    def "It persists a message"() {
        setup:
        def repository = Mock(MembershipRepository)
        AdvertiserAcceptedMembershipApplicationEventHandler handler = new AdvertiserAcceptedMembershipApplicationEventHandler(repository)
        def event = Mock(AdvertiserAcceptedMembershipApplicationEvent)
        event.data = new Data()
        event.data.publisherId = 1337
        event.data.advertiserId = 42
        def membership = Mock(Membership)

        when:
        handler.handle(event)

        then:
        1 * repository.getByKey(_ as MembershipId) >> membership
        1 * membership.activate() >> membership
        1 * repository.persist(membership)
    }
}