#!/bin/bash

#sbt package -v
#$SPARK_HOME/bin/spark-submit --class "SimpleApp" --master local[2] target/scala-2.10/energy-project_2.10-1.0.jar 

sbt assembly -v
$SPARK_HOME/bin/spark-submit --class "KafkaReceiver" target/scala-2.10/energy-project-assembly-1.0.jar
