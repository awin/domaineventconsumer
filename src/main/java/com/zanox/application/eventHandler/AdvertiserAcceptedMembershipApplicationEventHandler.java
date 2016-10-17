package com.zanox.application.eventHandler;

import com.zanox.application.DomainEventHandler;
import com.zanox.application.UnableToHandleEvent;
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;
import com.zanox.application.model.Membership;
import com.zanox.application.model.MembershipId;
import com.zanox.application.persistence.MembershipRepository;
import com.zanox.application.persistence.UnableToFindMembership;

public class AdvertiserAcceptedMembershipApplicationEventHandler implements DomainEventHandler<AdvertiserAcceptedMembershipApplicationEvent>
{
    private final MembershipRepository membershipRepository;

    public AdvertiserAcceptedMembershipApplicationEventHandler() {
        this.membershipRepository = new MembershipRepository();
    }

    public AdvertiserAcceptedMembershipApplicationEventHandler(MembershipRepository membershipRepository) {
        this.membershipRepository = membershipRepository;
    }

    public void handle(AdvertiserAcceptedMembershipApplicationEvent event) throws UnableToHandleEvent
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
            membership.activate()
        );
    }
}
