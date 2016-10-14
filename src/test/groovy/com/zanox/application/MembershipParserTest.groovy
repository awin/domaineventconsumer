package com.zanox.application

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
}