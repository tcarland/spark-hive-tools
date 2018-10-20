#!/usr/bin/env bash
#
# spark-submit for DbValidate


APP_JAR="target/spark-hive-tools-0.1.9-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.DbValidate"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
