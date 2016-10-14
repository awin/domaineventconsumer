package com.zanox.application;

import com.zanox.application.infrastructure.Parser;
import com.zanox.application.model.Membership;

public class MembershipParser implements Parser<Membership> {

    @Override
    public Membership parse(byte[] bytes) {
        Membership dto = new Membership(bytes);
        return dto;
    }
}
