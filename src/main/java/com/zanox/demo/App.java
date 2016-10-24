package com.zanox.demo;

public class App {

    public static void main(String[] args) {
        Processor processor = new Processor();
        com.zanox.kafka.highlevelconsumer.App app = new com.zanox.kafka.highlevelconsumer.App(processor);

        String zookeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int numTheads = Integer.parseInt(args[3]);
        app.run(zookeeper, groupId, topic, numTheads);
    }
}
