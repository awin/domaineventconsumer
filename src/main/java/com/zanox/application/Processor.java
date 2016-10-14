package com.zanox.application;

import com.zanox.application.infrastructure.Mapper;
import com.zanox.application.infrastructure.Parser;
import com.zanox.application.model.Membership;

public class Processor {
    private Parser<Membership> parser;
    private Mapper<Membership> mapper;

    public Processor() {
        parser = new MembershipParser();
        mapper = new MembershipMapper();
    }

    public Processor(Mapper<Membership> mapper, Parser<Membership> parser) {
        parser = parser;
        mapper = mapper;
    }

    public void process(byte[] message) {
        Membership membership = null;
        try {
            membership = parser.parse(message);
            mapper.persist(membership);
        } catch (BadMessageException e) {
            System.err.println("Unable to parse message");
        }
    }
}
