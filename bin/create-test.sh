#!/bin/bash
#
#  Spark submit script for the HiveTableSwapper
#

SRC_TABLE="$1"

APP_JAR="target/spark-hive-tools-test-0.1.6.jar"
APP_CLASS="HiveCreateTest"


spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $SRC_TABLE

