#!/bin/bash
#
#  Spark submit script for the HiveTableSwapper
#

APP="com.trace3.spark.HiveTableSwapper"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP \
  $APP_JAR \
  $@
