package com.zanox.kafka.durable.test;

import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.server.ServerCnxnFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EmbeddedKafka {

    private boolean isStarted = false;

    private int kafkaPort;
    private int zookeeperPort;

    private KafkaServer kafkaServer;
    private TestingServer zookeeperServer;

    public void start() throws Exception {
        if (isStarted) {
            throw new RuntimeException("Embedded Kafka has already started");
        }

        isStarted = true;

        startZookeeper();
        startKafka();
    }

    public void stop() {
        if (!isStarted) {
            throw new RuntimeException("Embedded Kafka has not started");
        }

        isStarted = false;

        stopKafka();
        stopZookeeper();
    }

    public String getKafkaConnectString() {
        if (!isStarted) {
            throw new RuntimeException("Embedded Kafka has not started");
        }

        return "localhost:" + kafkaPort;
    }

    public String getZookeeperConnectString() {
        if (!isStarted) {
            throw new RuntimeException("Embedded Kafka has not started");
        }

        return "localhost:" + zookeeperPort;
    }

    private void startZookeeper() throws Exception {

        zookeeperPort = getAvailablePort();

        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        zookeeperServer = new TestingServer(zookeeperPort, tempDir);
        zookeeperServer.start();
    }

    private void startKafka() throws Exception {

        kafkaPort = getAvailablePort();

        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        Properties properties = new Properties();
        properties.put("auto.create.topics.enable", "false");
        properties.put("broker.id", "1");
        properties.put("log.dir", tempDir.getAbsolutePath());
        properties.put("port", String.valueOf(kafkaPort));
        properties.put("zookeeper.connect", zookeeperServer.getConnectString());

        KafkaConfig kafkaConfig = new KafkaConfig(properties);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
        kafkaServer.startup();
    }

    private void stopKafka() {
        try {
            kafkaServer.shutdown();
        } catch (Exception e) {
        }
    }

    private void stopZookeeper() {
        try {
            zookeeperServer.close();
        } catch (Exception e) {
        }
    }

    private int getAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private class MockTime implements Time {

        private long nanos = 0;

        public MockTime() {
            nanos = System.nanoTime();
        }

        public long milliseconds() {
            return TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
        }

        public long nanoseconds() {
            return nanos;
        }

        public void sleep(long ms) {
            nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
        }
    }
}