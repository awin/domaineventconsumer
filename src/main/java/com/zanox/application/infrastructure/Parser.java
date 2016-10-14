package com.zanox.application.infrastructure;

import com.zanox.application.BadMessageException;

public interface Parser <T> {
    T parse(byte[] bytes) throws BadMessageException;
}
