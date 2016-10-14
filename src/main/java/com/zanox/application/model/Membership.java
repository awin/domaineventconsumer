package com.zanox.application.model;

public class Membership {
    private String string;
    public Membership(byte[] bytes) {
        string = new String(bytes);
    }

    public String getString() {
        return string;
    }
}
