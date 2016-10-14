package com.zanox.application

import com.zanox.application.infrastructure.Parser
import com.zanox.application.infrastructure.Mapper
import com.zanox.application.model.Membership
import spock.lang.Specification

class ProcessorTest extends Specification {
    def mapper
    def parser
    def setup() {
        mapper = Mock(Mapper)
        parser = Mock(Parser)
    }

    def "Processor can process a message"() {
        setup:
        Processor p = new Processor(mapper, parser)
        message = "foo".bytes
        membership = Mock(Membership)


        when:
        p.process(message)

        then:
        1 * parser.parse(message) >> membership
        1 * mapper.persist(membership)
    }

    def "Processor doesn't persist on error"() {
        setup:
        Processor p = new Processor(mapper, parser)
        message = "foo".bytes
        membership = Mock(Membership)

        given:
        parser.parse(message) >> {
            throw new BadMessageException()
        }
        when:
        p.process(message)

        then:
        0 * mapper.persist(_)
    }
}