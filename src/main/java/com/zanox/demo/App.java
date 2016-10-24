package com.zanox.demo;

public class App {

    public static void main(String[] args) {
        Processor processor = new Processor();
        com.zanox.kafka.highlevelconsumer.App app = new com.zanox.kafka.highlevelconsumer.App(processor);
        app.run(args);
    }
}
