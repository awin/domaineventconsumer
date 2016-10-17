package com.zanox.application.eventHandler;

import com.zanox.application.DomainEventHandler;
import com.zanox.application.RedisMapper;
import com.zanox.application.UnableToHandleEvent;
import com.zanox.application.event.AdvertiserAcceptedMembershipApplicationEvent;

public class AdvertiserAcceptedMembershipApplicationEventHandler implements DomainEventHandler<AdvertiserAcceptedMembershipApplicationEvent>
{
    private final RedisMapper redisMapper;

    public AdvertiserAcceptedMembershipApplicationEventHandler(RedisMapper redisMapper) {
        this.redisMapper = redisMapper;
    }

    public void handle(AdvertiserAcceptedMembershipApplicationEvent event) throws UnableToHandleEvent
    {
        String key = String.format("%s-%s", event.advertiserId, event.publisherId);
        String status = "accepted";
        redisMapper.persist(key, status);
    }
}
