package integration.helper;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;

import java.util.Properties;

public class EmbeddedKafkaProducer {

    private final EmbeddedKafka embeddedKafka;

    private boolean isInitialized = false;

    private Producer<String, String> producer;
    private ZkClient zkClient;
    private ZkUtils zkUtils;

    public EmbeddedKafkaProducer(
        EmbeddedKafka embeddedKafka
    ) {
        this.embeddedKafka = embeddedKafka;
    }

    public void createTopic(
        String topic
    ) {
        initialize();

        AdminUtils.createTopic(
            zkUtils,
            topic,
            2,
            1,
            new Properties()
        );
    }

    public void createTopic(
        String topic,
        Integer numOfPartitions
    ) {
        initialize();

        AdminUtils.createTopic(
            zkUtils,
            topic,
            numOfPartitions,
            1,
            new Properties()
        );
    }

    public void sendMessage(
        String topic,
        Integer key,
        String message
    ) {
        sendMessage(
            topic,
            key.toString(),
            message
        );
    }

    public void sendMessage(
        String topic,
        String key,
        String message
    ) {
        initialize();

        producer.send(new KeyedMessage<>(topic, key, message));
    }

    private void initialize() {
        if (isInitialized) {
            return;
        }

        isInitialized = true;

        Properties properties = new Properties();
        properties.put("batch.num.messages", "1");
        properties.put("metadata.broker.list", embeddedKafka.getKafkaConnectString());
        properties.put("producer.type", "sync");
        properties.put("request.required.acks", "1");
        properties.put("topic.metadata.refresh.interval.ms", "100");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        producer = new Producer<>(new ProducerConfig(properties));

        int retries = 5;

        do {

            try {

                ZkConnection connection =
                    new ZkConnection(
                        embeddedKafka.getZookeeperConnectString()
                    );

                zkClient = new ZkClient(
                    connection,
                    500,
                    ZKStringSerializer$.MODULE$
                );

                zkUtils = new kafka.utils.ZkUtils(zkClient, connection, false);

                break;

            } catch (ZkTimeoutException e) {

                if (retries == 0) {
                    throw e;
                }

                System.err.println("Failed to connect to Zookeeper, retrying..., " + retries + " left");
            }

        } while (retries-- > 0);
    }
}