#!/bin/bash
#
#  Spark submit script for the HiveTableSwapper
#

SRC_TABLE="$1"

APP_JAR="target/spark-hive-tools-0.1.3-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.HiveTableCheck"


spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $SRC_TABLE

