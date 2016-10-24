package com.zanox.demo.eventHandler;

import com.zanox.demo.event.AdvertiserSuspendedMembershipEvent;
import com.zanox.demo.model.Membership;
import com.zanox.demo.model.MembershipId;
import com.zanox.application.persistence.MembershipRepository;
import com.zanox.application.persistence.UnableToFindMembership;

public class AdvertiserSuspendedMembershipEventHandler
{
    private final MembershipRepository membershipRepository;

    public AdvertiserSuspendedMembershipEventHandler() {
        this.membershipRepository = new MembershipRepository();
    }

    public AdvertiserSuspendedMembershipEventHandler(MembershipRepository membershipRepository) {
        this.membershipRepository = membershipRepository;
    }

    public void handle(AdvertiserSuspendedMembershipEvent event) throws UnableToHandleEvent
    {
        Membership membership;
        MembershipId membershipId = new MembershipId(event.advertiserId, event.publisherId);

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
