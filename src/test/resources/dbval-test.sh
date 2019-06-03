#!/usr/bin/env bash
#
#  Runs a DBValidate test to compare a parquet table to
#  a mysql table
#

DBVALIDATE="bin/dbvalidate.sh"
host="$1"

if [ -z "$host" ]; then
  echo " Usage: $0  <mysql_host:port>"
  echo "   Please provide the hostname for the mysql server"
  exit 0
fi


$DBVALIDATE --user sht --password shttester \
--jdbc jdbc:mysql://${host}/sht_test \
--driver com.mysql.jdbc.Driver \
--dbtable sht_test.testdata1 \
--dbkey FLOWID \
--hive-table default.sht_testdata2 \
--sumcols BYTES,PKTS
