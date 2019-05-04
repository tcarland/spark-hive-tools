#!/usr/bin/env bash
#
# spark-submit for DbValidate
#

APP="com.trace3.spark.DbValidate"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP \
  $APP_JAR \
  $@
