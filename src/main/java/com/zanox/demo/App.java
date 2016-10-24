package com.zanox.demo;

public class App {

    public static void main(String[] args) {
        Processor processor = new Processor();
        com.zanox.kafka.highlevelconsumer.App app = new com.zanox.kafka.highlevelconsumer.App(processor);

        String zkHost = args[0];
        String groupId = args[1];
        String topic = args[2];
        int numTheads = Integer.parseInt(args[3]);
        app.run(zkHost, groupId, topic, numTheads);
    }
}
