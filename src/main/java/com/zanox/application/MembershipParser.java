package com.zanox.application;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.zanox.application.infrastructure.Parser;
import com.zanox.application.model.Membership;

public class MembershipParser implements Parser<Membership> {

    private Gson gson;

    public MembershipParser() {
        gson = new Gson();
    }

    @Override
    public Membership parse(byte[] bytes) throws BadMessageException {
        Membership dto = null;
        try {
            dto = gson.fromJson(new String(bytes), Membership.class);
        } catch (JsonParseException e) {
            throw new BadMessageException(e);
        }
        return dto;
    }
}
