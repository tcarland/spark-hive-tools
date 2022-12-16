#!/bin/bash
#
#  Spark submit script for the HiveCreateTblTest
#

APP_JAR="target/spark-hive-tools-test-0.6.1.jar"
APP_CLASS="HiveCreateTblTest"


spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $@
