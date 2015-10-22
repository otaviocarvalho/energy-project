#!/bin/bash
source ~/.bashrc

# Zookeeper
$ZOOKEEPER_HOME/bin/zkServer.sh stop
sleep 1

# Kafka
$KAFKA_HOME/bin/kafka-server-stop.sh

# Redis
sudo service redis-server stop
