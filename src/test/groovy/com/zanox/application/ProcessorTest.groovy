package com.zanox.application

import spock.lang.Specification

class ProcessorTest extends Specification {
    def "Processor can process a message"() {
        setup:
        def parser = Mock(EventParser)
        def handlerFactory = Mock(EventHandlerFactory)
        Processor p = new Processor(parser, handlerFactory)
        def message = "foo".bytes
        def event = Mock(DomainEvent)
        def handler = Mock(DomainEventHandler)


        when:
        p.process(message)

        then:
        1 * parser.getEventFromMessage(message) >> event
        1 * handlerFactory.getHandlerByEvent(event) >> handler
        1 * handler.handle(event)
    }

    def "Processor doesn't persist on error"() {
        setup:
        def parser = Mock(EventParser)
        def handlerFactory = Mock(EventHandlerFactory)
        Processor p = new Processor(parser, handlerFactory)
        def message = "foo".bytes

        when:
        p.process(message)

        then:
        0 * handlerFactory.getHandlerByEvent(_)
        parser.getEventFromMessage(message) >> {
            throw new BadMessageException()
        }
    }
}