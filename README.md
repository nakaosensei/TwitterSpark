# TwitterSpark

## Python dependencies
[Packages](https://github.com/nakaosensei/TwitterSpark/blob/master/requeriments.txt)
### Virtual enviroment(optional)
```console
me@linux:~$ python3 -m venv virtualenv
me@linux:~$ source virtualenv/bin/activate
me@linux:~$ pip3 install -r requeriments.txt
#deactivate to leave env
```
## Downloading Everything

 - [Apache Spark.](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz)
 - [Apache Kafka.](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz)

Extract everything anywhere you want.kaf

## Environment and System Architecture


## Initiating Apache Kafka

Let's initiate `Zookeeper manager`, `Kafka broker`, and create some `Kafka topics` for our application!

### Zookeeper Service

```console
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/zookeeper-server-start.sh /PATH/TO/kafka_2.12-2.3.0/config/zookeeper.properties
```

### Kafka Service
```console
me@linux:~$ sudo /PATH/TO/kafka_2.12-2.3.0/bin/kafka-server-start.sh /PATH/TO/kafka_2.12-2.3.0/config/server.properties
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

Please modify `Line 3` in file `spark_subscriber.py` with the *path* that you specified for Apache Spark.

```console
me@linux:~$ python spark_subscriber.py
```
