package com.zanox.application;

import com.zanox.application.parser.EventParser;

public class Processor {
    private EventParser parser;

    public Processor() {
        parser = new EventParser();
    }

    public Processor(EventParser parser) {
        this.parser = parser;
    }

    public void process(byte[] message) {
        parser.handle(message);
    }
}
