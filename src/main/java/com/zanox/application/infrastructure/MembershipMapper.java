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
        Long value = jedis.incr(dto.getString());
        System.out.println("Value at" + dto.getString() + " : " + value);

    }
}
