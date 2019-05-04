#!/usr/bin/env bash
#
# spark-submit for DbLocations
#

APP="com.trace3.spark.DbTableLocations"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP \
  $APP_JAR \
  $@
