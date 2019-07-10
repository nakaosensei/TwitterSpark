#!/usr/bin/env bash

# store arguments in a special array
args=("$@")
# get number of topics
ELEMENTS=${#args[@]}

for (( i=0;i<$ELEMENTS;i++)); do
    ~/GitHub/TwitterSpark/lib/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ${args[${i}]}
done