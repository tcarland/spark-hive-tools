#!/bin/bash
#
#  Spark submit script for the ParquetValidate
#

APP="com.trace3.spark.ParquetValidate"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP \
  $APP_JAR \
  $@
