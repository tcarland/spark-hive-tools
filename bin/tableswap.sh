#!/bin/bash
#
#  Spark submit script for the HiveTableSwapper
#

SRC_TABLE="$1"
DST_TABLE="$2"
NUM_PARTS="$3"
SPLIT_COL="$4"

APP_JAR="target/spark-hive-tools-0.1.7-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.HiveTableSwapper"

if [ -z "$SRC_TABLE" ] || [ -z "$DST_TABLE" ]; then
    echo "Error: not enough parameters"
    echo "Usage: $0 [src_table] [dst_table] <num_of_partitions> <splitby-col>"
    exit 1
fi

spark-submit --master yarn \
  --deploy-mode client \
  --num-executors 4 \
  --executor-cores 1 \
  --executor-memory 1g \
  --class $APP_CLASS \
  $APP_JAR \
  $SRC_TABLE $DST_TABLE $NUM_PARTS $SPLIT_COL


