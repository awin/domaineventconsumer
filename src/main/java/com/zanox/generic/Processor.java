package com.zanox.generic;

import com.zanox.generic.eventHandler.ErrorInHandlerException;
import com.zanox.generic.parser.BrokenMessageFormatException;

public interface Processor {
    void process(byte[] message) throws BrokenMessageFormatException, ErrorInHandlerException;
}
