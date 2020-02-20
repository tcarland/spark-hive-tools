#!/usr/bin/env bash
#
# spark-submit for DbLocations
#

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

APP_CLASS="com.trace3.spark.DbTableLocations"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
