# kafka-highlevel-consumer
An implementation of the high level kafka consumer

### Run with:  
```
mvn package && java -cp target/highlevelconsumer.jar com.zanox.kafka.highlevelconsumer.App {zookeeper} {group-id} {topic} {num-of-threads}
```

**{zookeeper}** - zookeeper host, e.g. localhost:2181  
**{group-id}** - consumer group id   
**{topic}** - name of the topic  
**{num-of-threads}** - number of threads to use for the consumer. If you specify more threads than the number of Kafka partitions, some of the threads won't be doing anything because Kafka never allow a single partition to be consumed from more than one thread.  

### Test it:
A good way to verify all threads work as expected, try to use the following kafka cmd tool 
 
```
watch {path-to-kafka}/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --topic {topic} --zookeeper {zookeeper} --group {group-id}
```

### How to set up a full test

Start with running the required infrastructure, Zookeeper, Kafka, Redis:

Net Host here is required, because we need to connect to Kafka from clients outside of Docker.
Kafka not really working well with Docker because of hostname problems.

```
docker run -d --net=host --name zookeeper wurstmeister/zookeeper
docker run -d --net=host --name k01 kafka
```

Kafka image is currently a patched wurstmeister image, found at `yarektyshchenko/kafka` on Github.

Create a topic `test`:
```
docker exec -it k01 bash
$KAFKA_HOME/bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --zookeeper localhost:2181 --topic test
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --describe
```

Start up Redis:

```
docker run --name redis -p 6379:6379 -d redis
docker logs -f redis
```

Now you can connect with your application.