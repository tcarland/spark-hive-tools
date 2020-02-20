#!/bin/bash
#
#  Spark submit script for the ParquetValidate
#

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/hive-tools-config.sh

APP_CLASS="com.trace3.spark.ParquetValidate"

spark-submit --master yarn \
  --deploy-mode client \
  --class $APP_CLASS \
  $APP_JAR \
  $@
