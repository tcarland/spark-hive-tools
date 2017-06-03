#!/bin/bash
#
#  Spark submit script for the ParquetValidate
#

APP_JAR="target/spark-hive-tools-0.1.3-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.ParquetValidate"

spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 4 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $@


