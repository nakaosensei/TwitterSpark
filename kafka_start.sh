#!/bin/bash

nohup ~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/bin/zookeeper-server-start.sh ~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/config/zookeeper.properties > /dev/null 2>&1 &
sleep 2
nohup ~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/bin/kafka-server-start.sh ~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/config/server.properties > /dev/null 2>&1 &
sleep 2

~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterEng
~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterPt