#!/bin/bash
source ~/.bashrc
PROJECT_HOME="/home/omcarvalho/tcc/project/spark/energy_project/"

# zookeeper
nohup $ZOOKEEPER_HOME/bin/zkServer.sh start $ZOOKEEPER_HOME/conf/zoo.cfg &
sleep 1

# kafka
rm -rf /tmp/kafka-logs/streaming-topic*
echo "rmr /brokers/streaming-topic" | $ZOOKEEPER_HOME/bin/zkCli.sh
sleep 1
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
sleep 1
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streaming-topic

## kafka-test
#echo "bla ble bli blo blu" | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streaming-topic
#$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic streaming-topic --from-beginning
#PID=$!
## Wait for 2 seconds
#sleep 2
## Kill it
#kill $PID

# redis
#sudo add-apt-repository ppa:chris-lea/redis-server
#sudo apt-get update
#sudo apt-get install redis-server
#redis-benchmark -q -n 1000 -c 10 -P 5
sudo service redis-server restart
redis-cli flushall

# run kafka producer
#nohup $SPARK_HOME/bin/spark-submit --class "KafkaInputProducer" target/scala-2.10/energy-projt-assembly-1.0.jar localhost:9092 streaming-topic 3 5 &

# run spark consumer
#$SPARK_HOME/bin/spark-submit --class "KafkaReceiver" target/scala-2.10/energy-project-assembly-1.0.jar
