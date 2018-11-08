#!/usr/bin/env bash

kafka-topics --zookeeper 127.0.0.1:2181 --list
kafka-topics --zookeeper 127.0.0.1:2181 --topic kafkaConnectStandalone --delete
kafka-topics --zookeeper 127.0.0.1:2181 --list
kafka-topics --create --topic kafkaConnectStandalone --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
kafka-topics --zookeeper 127.0.0.1:2181 --list