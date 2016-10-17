package com.zanox.application;

import com.zanox.application.infrastructure.Mapper;
import redis.clients.jedis.Jedis;

public class RedisMapper implements Mapper {
    private Jedis jedis;
    public RedisMapper() {
        jedis = new Jedis("d-lhr1-docker-001.zanox.com");
    }

    @Override
    public void persist(String key, String status) {
        jedis.set(key, status);
    }
}
