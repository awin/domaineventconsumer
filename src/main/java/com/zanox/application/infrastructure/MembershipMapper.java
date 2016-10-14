package com.zanox.application.infrastructure;

import com.zanox.application.model.Membership;
import redis.clients.jedis.Jedis;

public class MembershipMapper implements Mapper<Membership> {
    private Jedis jedis;
    public MembershipMapper() {
        jedis = new Jedis("d-lhr1-docker-001.zanox.com");
    }
    @Override
    public void persist(Membership dto) {
        jedis.set("foo", dto.getString());
        String value = jedis.get("foo");

    }
}
