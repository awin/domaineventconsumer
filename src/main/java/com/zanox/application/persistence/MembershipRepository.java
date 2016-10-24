package com.zanox.application.persistence;

import com.zanox.demo.model.Membership;
import com.zanox.demo.model.MembershipId;
import redis.clients.jedis.Jedis;

public class MembershipRepository {
    private final Jedis jedis;

    public MembershipRepository() {
        // @todo refactor
        this.jedis = new Jedis("d-lhr1-docker-141.zanox.com");
    }

    public Membership getByKey(MembershipId id) throws UnableToFindMembership {
        String key = String.format("%s-%s", id.advertiserId, id.publisherId);
        String status = jedis.get(key);

        if (status == null) {
            throw new UnableToFindMembership();
        }

        return new Membership(id, status);
    }

    public MembershipRepository persist(Membership membership) {
        String key = String.format("%s-%s", membership.id.advertiserId, membership.id.publisherId);
        jedis.set(key, membership.status);

        return null;
    }
}
