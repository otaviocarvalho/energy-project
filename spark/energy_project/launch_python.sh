#!/bin/bash

$SPARK_HOME/bin/spark-submit --class "PythonPi" --master local[2] ./src/main/python/pi.py 4
