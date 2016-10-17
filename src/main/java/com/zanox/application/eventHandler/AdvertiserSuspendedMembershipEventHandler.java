package com.zanox.application.eventHandler;

import com.zanox.application.DomainEventHandler;
import com.zanox.application.UnableToHandleEvent;
import com.zanox.application.event.AdvertiserSuspendedMembershipEvent;
import com.zanox.application.model.Membership;
import com.zanox.application.model.MembershipId;
import com.zanox.application.persistence.MembershipRepository;
import com.zanox.application.persistence.UnableToFindMembership;

public class AdvertiserSuspendedMembershipEventHandler implements DomainEventHandler<AdvertiserSuspendedMembershipEvent>
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
