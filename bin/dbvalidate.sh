#!/usr/bin/env bash
#
# spark-submit for DbValidate
#

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

APP_CLASS="com.trace3.spark.DbValidate"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
