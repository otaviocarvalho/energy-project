#!/bin/bash
source ~/.bashrc

# zookeeper
nohup $ZOOKEEPER_HOME/bin/zkServer.sh start conf/zoo.cfg &

# kafka
nohup $KAFKA_HOME/bin/kafka-server-start.sh config/server.properties &
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streaming-topic

# kafka-test
echo "bla ble bli blo blu" > $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streaming-topic
./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic streaming-topic --from-beginning &
PID=$!
# Wait for 2 seconds
sleep 2
# Kill it
kill $PID

# kafka producer
nohup $SPARK_HOME/bin/spark-submit --class "KafkaInputProducer" target/scala-2.10/energy-projt-assembly-1.0.jar localhost:9092 streaming-topic 3 5 &

# spark consumer
$SPARK_HOME/bin/spark-submit --class "KafkaReceiver" target/scala-2.10/energy-project-assembly-1.0.jar
