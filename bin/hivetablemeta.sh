#!/usr/bin/env bash
#
# spark-submit for HiveTableMeta


APP_JAR="target/spark-hive-tools-0.1.9-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.HiveTableMeta"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
