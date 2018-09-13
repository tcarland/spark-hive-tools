#!/bin/bash
#
#  Spark submit script for the HiveTableSwapper
#

APP_JAR="target/spark-hive-tools-0.1.8-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.HiveTableSwapper"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
