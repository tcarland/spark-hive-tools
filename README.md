spark-hive-tools
================


##### Overview

  Provides convenient Scala functions for interacting or performing common operations on Hive tables.


 * HiveTableSwapper - Move tables with an optional re-partition.
 * ParquetValidate  - Compare schemas across parquet partitions (ie. schema evolution).
 * DBValidate       - Compare counts and schema of a hive table to an external jdbc database.

##### HiveTableSwapper

A tool intended for the post-injestion process of moving a new table into place of an existing 
table; optionally allowing for a table repartition in the process.

More specifically, occasionally in certain large scale RDMS environments, the odd schema design 
may lack a column to split-by or that can be relied on for running incremental exports.  In case 
where a given table is not so large, it can be relatively cheap enough to ingest the entire table 
and then swap the table in place.  
 
  Sqoop also has the issue of partitioning in this situation.  Without a column that can be used 
for ranged queries, the resulting import ends up with unbalanced partitions. This tool allows 
for the optional repartitioning of a table via Spark (using it's HashedPartitioner) that will 
redistribute the partitions more evenly.

  This may be a specific use case, but this also serves as a good example of some basic Hive 
interactions from spark additionally demonstrating a workaround to the compatability issues 
between Spark, Hive and Parquet. Spark uses a custom column 'SerDe' when writing parquet which 
results in tables being unusable from Hive or Impala. Notably, any use of .saveAsTable() including 
APPEND mode will rewrite the metadata. To avoid this one first runs CREATE TABLE via Hive and then 
uses DataSet.insertInto() versus DataSet.saveAsTable().

 - NOTE: Renaming a Table via ALTER TABLE is only cheap if the table is not moving databases. It 
 is best to not use a different schema/db name between the source and destination tables, as the 
 RENAME operation may result in a full copy. HiveTableSwapper in fact assumes this to be true and 
 only modifies the Hive physical LOCATION to account for the new table name in the path, not the 
 db. If different databases were used, the table would be in the wrong location.
 
 - NOTE: The repartitioning step rewrites the source table via Spark into a temporary table that 
 is then renamed to the destination.

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
 --split-by=ACCT_NO --hive-import --hive-database=PBX 
 --hive-table=PBX.GET_LIMIT_VTMP 
 --username $DBUSER --password-file $DBPASSFILE

r=$?

return $r
``` 


<!--
 * Repartitioner 
--> 

##### ParquetValidate
 
 Iterates on a Parquet Table's Partitions and reports on missing columns (usually as a result of 
 schema evolution).


##### DBValidate

Compares the columns of an external database table (via JDBC) to a given Hive Table with the 
option of comparing column values by running a sum of n cols aross y rows.


Testing DbValidate:

   The test for DBValidate uses a mysql instance to run the comparison. The following will seed the 
test data in both MySQL and Hive for running the test app.

```
  $ mysql -u root -p < src/test/resources/sht-mysql-init.sql
  $ hadoop fs -put src/test/resources/sht_data1.csv
  $ hadoop fs -put src/test/resources/sht_data2.csv
  $ ./src/test/resources/dbval-init.sh mysqlhost:port  
```
  To build the jar for testing, first compile via ***mvn package*** followed by running the script
**src/test/resources/build-test-jar.sh**

  Run the dbval-test.sh script providing the hostname of the external mysql server to run the test.
```
  ./bin/dbval-test.sh mydbhost:3306

```


