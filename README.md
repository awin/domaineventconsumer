# Domain Event Consumer Library
This is a library to help build Consumers that connect to Kafka

It contains two implementations of consumers, one with automatic commit and another that
lets you track offsets yourself, for better durability.

```
# build with
mvn package
```

### Try High level consumer:
```
java -cp target/highlevelconsumer.jar com.zanox.kafka.highlevelconsumer.App {zookeeper} {group-id} {topic} {num-of-threads}
```

**{zookeeper}** - zookeeper host, e.g. localhost:2181  
**{group-id}** - consumer group id   
**{topic}** - name of the topic  
**{num-of-threads}** - number of threads to use for the consumer. If you specify more threads than the number of Kafka partitions, some of the threads won't be doing anything because Kafka never allow a single partition to be consumed from more than one thread.  

### Try durable consumer:
```
java -cp target/domaineventconsumer.jar com.zanox.generic.PoCDurableConsumer {topic} {seed_kafka_node}
```
**{seed_kafka_node}** - Just one of available kafka nodes

Internally the consumer will autodiscover all available nodes

### Test it:
A good way to verify all threads work as expected, try to use the following kafka cmd tool 
 
```
watch {path-to-kafka}/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --topic {topic} --zookeeper {zookeeper} --group {group-id}
```
