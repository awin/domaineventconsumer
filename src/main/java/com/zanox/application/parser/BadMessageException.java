package com.zanox.application.parser;

public class BadMessageException extends Exception {
    public BadMessageException(Throwable e) {
        super(e);
    }
    public BadMessageException() {}
}
