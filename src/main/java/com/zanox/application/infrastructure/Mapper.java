package com.zanox.application.infrastructure;

public interface Mapper <T> {
    void persist(T dto);
}
