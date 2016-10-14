package com.zanox.application.infrastructure;

public interface Parser <T> {
    T parse(byte[] bytes);
}
