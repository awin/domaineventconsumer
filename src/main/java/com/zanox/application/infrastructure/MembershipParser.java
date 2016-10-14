package com.zanox.application.infrastructure;

import com.zanox.application.model.Membership;

public class MembershipParser implements Parser <Membership> {

    @Override
    public Membership parse(byte[] bytes) {
        Membership dto = new Membership(bytes);
        return dto;
    }
}
