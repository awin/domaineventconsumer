package com.zanox.application

import com.zanox.application.model.Membership
import spock.lang.Specification

class MembershipParserTest extends Specification {
    def "it rejects non json"() {
        setup:
        def parser = new MembershipParser()

        when:
        parser.parse("not JSON".bytes)

        then:
        thrown BadMessageException
    }

    def "It parses affiliateId"() {
        setup:
        def parser = new MembershipParser();

        when:
        Membership m = parser.parse("{affiliateId: 42}".bytes)

        then:
        assert m.affiliateId == 42
    }
}