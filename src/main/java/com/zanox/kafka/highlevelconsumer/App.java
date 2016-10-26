package com.zanox.kafka.highlevelconsumer;

import com.zanox.generic.Processor;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Future;

public class App {

    private Processor processor;

    public App(Processor processor) {
        this.processor = processor;
    }

    public void run(String zookeeper, String groupId, String topic, int numberOfThreads) {
        System.out.println("zookeeper: " + zookeeper);
        System.out.println("groupId: " + groupId);
        System.out.println("topic: " + topic);
        System.out.println("number of threads: " + numberOfThreads);

        //reset zookeeper offset to the beginning
        //new ZkOffsetReseter(zookeeper, 2181, groupId).reset();

        ConsumerConfig consumerConfig = ConsumerConfigFactory.create(zookeeper, groupId);
        ConsumerConnector consumerConnector = new ConsumerConnectorFactory(consumerConfig).create();

        ConsumerExecutor consumerExecutor = new ConsumerExecutor(
            new ConsumerFactory(this.processor),
            new MessageStreamFactory(topic, consumerConnector),
            new ExecutorServiceFactory());
        Collection<Future> futureSessions = consumerExecutor.run(numberOfThreads);

        Iterator<Future> iterator = futureSessions.iterator();
        while (iterator.hasNext()) {
            Future nextSession = iterator.next();
            while (!nextSession.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    if (!nextSession.isCancelled()) {
                        nextSession.cancel(true);
                        System.out.println("Thread has been interrupted, "
                                + nextSession.toString() + " has been cancelled");
                    }
                }
            }
        }

        System.out.println("All consumer threads have been stopped");

    }
}
