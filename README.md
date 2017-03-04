spark-hive-tools
================


#### Overview

  Provides convenient Scala functions for interacting or performing common operations on Hive tables.


 * HiveTableSwapper 

  A tool intended for the post-injestion process of moving a new table into place of an existi
ng table; optionally allowing for a table repartition in the process.

  More specifically, occasionally in certain large scale RDMS environments, the odd schema des
ign may lack a column to split-by or that can be relied on for running incremental exports.  I
n case where a given table is not so large, it can be relatively cheap enough to ingest the en
tire table and then swap the table in place.  
 
  Sqoop also has the issue of partitioning in this situation.  Without a column that can be us
ed for ranged queries, the resulting import ends up with unbalanced partitions. This tool allo
ws for the optional repartitioning of a table via Spark (using it's HashedPartitioner) that wi
ll redistribute the partitions more evenly.

  This may be a specific use case, but this also serves as a good example of some basic Hive i
nteractions from spark additionally demonstrating a workaround to the compatability issues bet
ween Spark, Hive and Parquet. Spark uses a custom column 'SerDe' when writing parquet which re
sults in tables being unusable from Hive or Impala. Notably, any use of .saveAsTable() includi
ng APPEND mode will rewrite the metadata. To avoid this one first runs CREATE TABLE via Hive a
nd then uses DataSet.insertInto() versus DataSet.saveAsTable().

 - NOTE: Renaming a Table via ALTER TABLE is only cheap if the table is not moving location or
 database. It is best to not use a different schema/db name between the source and destination
 tables, as the RENAME operation may result in a full copy. HiveTableSwapper in fact assumes t
his to be true and only modifies the Hive physical LOCATION to account for the new table name 
in the path, not the db. If different databases were used, the table would be in the wrong loc
ation.
 
 - NOTE: The repartitioning step rewrites the source table via Spark into a temporary table th
at is then renamed to the destination.

 - Sqoop example:

```
#!/bin/bash

DBUSER="$1"
DBPASSFILE="$2"

if [ -z "$DBPASSFILE" ]; then
    echo "ERROR: Password file not provided"
    exit 1
fi

sqoop import --connect jdbc:oracle:thin:@orapita-db:1521/dev_name_con -m 8 \
 --table=PBX.GET_LIMIT_V --as-parquetfile --compression-codec=snappy \
 --split-by=ACCT_NO --hive-import --hive-database=risk 
 --hive-table=PBX.GET_LIMIT_VTMP 
 --username $DBUSER --password-file $DBPASSFILE

r=$?

return $r
``` 

 * Repartitioner

