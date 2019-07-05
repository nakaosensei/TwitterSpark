# TwitterSpark

## Downloading Everything

 - [Apache Spark.](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz)
 - [Apache Kafka.](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz)

Extract everything anywhere you want.

## Environment and System Architecture


## Initiating Apache Kafka

### Zookeeper Service

```console
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/kafka-server-start.sh /PATH/TO/kafka_2.12-2.3.0/config/server.properties
```

### Kafka Service

```console
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/zookeeper-server-start.sh /PATH/TO/kafka_2.12-2.3.0/config/zookeeper.properties
```

### Creating Topics

```console
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterEng
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterPt
```

## Running the Publisher

```console
me@linux:~$ python spark_publisher.py
```

## Running the Subscriber

```console
me@linux:~$ python spark_subscriber.py
```
