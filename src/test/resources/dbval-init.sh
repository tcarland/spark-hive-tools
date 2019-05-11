#!/bin/bash
#
#  Spark submit script for initializing test data
#


APP_JAR="target/spark-hive-tools-test-0.2.4.jar"
APP_CLASS="SHTTestInit"

spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $@
