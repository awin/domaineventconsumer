package com.zanox.demo.eventHandler;

import com.zanox.generic.eventHandler.ErrorInHandlerException;
import com.zanox.demo.event.PublisherLeftProgrammeEvent;
import com.zanox.demo.model.Membership;
import com.zanox.demo.model.MembershipId;
import com.zanox.demo.persistence.MembershipRepository;
import com.zanox.demo.persistence.UnableToFindMembership;

public class PublisherLeftProgrammeEventHandler {
    private final MembershipRepository membershipRepository;

    public PublisherLeftProgrammeEventHandler() {
        this.membershipRepository = new MembershipRepository();
    }

    public PublisherLeftProgrammeEventHandler(MembershipRepository membershipRepository) {
        this.membershipRepository = membershipRepository;
    }

    public void handle(PublisherLeftProgrammeEvent event) throws ErrorInHandlerException
    {
        Membership membership;
        MembershipId membershipId = new MembershipId(event.data.advertiserId, event.data.publisherId);

        // load or create if does not exist
        try {
            membership = membershipRepository.getByKey(membershipId);
        } catch (UnableToFindMembership e) {
            membership = new Membership(membershipId);
        }

        // activate & persist
        membershipRepository.persist(
            membership.deactivate()
        );
    }

}
