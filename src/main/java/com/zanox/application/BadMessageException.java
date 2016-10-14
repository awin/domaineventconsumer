package com.zanox.application;

public class BadMessageException extends Exception {
    public BadMessageException(Throwable e) {
        super(e);
    }
    public BadMessageException() {}
}
