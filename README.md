# Domain Event Consumer Library
A library to help build exactly-once semantics Kafka Consumers

While there are plenty of consumer libraries that are of the automatic nature
this library is designed for building applications that exhibit no message loss
during failures. This is achieved by letting the Application track offsets itself
instead of relying on Kafka Commit API or Zookeeper. The reasoning is that in a
distributed system its near impossible to achieve consensus between two systems.

By allowing you to save message offset in the same transaction as processing of the
message ensures that if anything fails, the consumer will be able to restart from
before the failed message.

### Try durable consumer:
```
java -cp target/domaineventconsumer.jar com.zanox.generic.PoCDurableConsumer {topic} {seed_kafka_node}
```
**{seed_kafka_node}** - Just one of available kafka nodes

Internally the consumer will autodiscover all available nodes

