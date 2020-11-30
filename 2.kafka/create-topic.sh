#!/usr/bin/env bash

cd /usr/lib/kafka


# Create a topic
bin/kafka-topics.sh --create \
  --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 13 \
  --topic bitcoin-transactions


  # Create a topic
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 13 --topic bitcoin-transactions