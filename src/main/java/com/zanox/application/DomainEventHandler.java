package com.zanox.application;

/**
 * Created by aram.karapetyan on 14/10/16.
 */
public interface DomainEventHandler<T> {
    void handle(T event) throws UnableToHandleEvent;
}
