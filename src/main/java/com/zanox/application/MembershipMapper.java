package com.zanox.application;

import com.zanox.application.infrastructure.Mapper;
import com.zanox.application.model.Membership;
import redis.clients.jedis.Jedis;

public class MembershipMapper implements Mapper<Membership> {
    private Jedis jedis;
    public MembershipMapper() {
        jedis = new Jedis("d-lhr1-docker-001.zanox.com");
    }
    @Override
    public void persist(Membership dto) {
        String key = String.format("%s-%s", dto.merchantId, dto.affiliateId);
        String status = dto.status;

        jedis.set(key, status);
    }
}
