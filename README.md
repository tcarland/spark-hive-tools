spark-hive-tools
================

## Overview

Provides convenient Scala functions for interacting or performing operations
on Hive tables via Spark2.

 * HiveTableSwapper - Move tables with an optional re-partition.
 * ParquetValidate  - Compare schemas across parquet partitions (ie. schema evolution).
 * DBValidate       - Compare schema of a hive table to an external jdbc database.
 * HiveTableMeta    - Create a file of table meta statements for a given database.
 * DBTableLocations - Compare external table locations with parent db location.

---
## HiveTableSwapper

A spark class intended for the post-injestion process of moving a new table 
into place of an existing table; optionally allowing for a table repartition 
in the process.

More specifically, in some RDMS environments, a given schema design may lack 
an obvious column to split-by or a column that can be relied on for running 
incremental exports.  In the case where a given table is not so large, it 
can be relatively cheap enough to ingest the entire table and then swap the 
table in place.  

Sqoop often has the issue of partitioning with split columns that are not 
evenly distributed. Without a column that can be used for ranged queries, 
the resulting sqoop import ends up with unbalanced partitions. This tool 
allows for an optional re-partition of a table via Spark, using its 
*HashedPartitioner*, that will redistribute the partitions more evenly.

This is a somewhat limited or specific use case, but also serves as a good 
example of some basic Hive interactions from Spark. Spark uses a custom 
column 'SerDe' when writing parquet which can result in tables being 
unusable from Hive or Impala.  Notably, any use of *.saveAsTable()* 
including APPEND mode will rewrite the metadata. To avoid this, we first 
run `CREATE TABLE` via Hive and then use *DataSet.insertInto()* versus 
*DataSet.saveAsTable()*.

- NOTE: Renaming a Table via `ALTER TABLE` is only cheap if the table is 
  not moving databases. It is best to *not* use a different schema/db name 
  between the source and destination tables, as the *RENAME* operation may 
  result in a full copy. HiveTableSwapper, in fact, assumes this to be true 
  and only modifies the Hive physical *LOCATION* to account for the new table 
  name, but does not consider the dbname within the location. If different 
  databases were used with this, the table would end up in the wrong HDFS 
  location.

- NOTE: The re-partitioning step rewrites the source table via Spark into a
  temporary table that is then renamed to the destination.


Sqoop example:
```bash
#!/usr/bin/env bash

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


## ParquetValidate

 Iterates on a Parquet Table's Partitions and reports on any missing columns,
usually a result of schema change or evolution feature in Parquet.


## DbValidate

Compares the columns of an external database table (via JDBC) to a given Hive
Table with the option of comparing column values by running a sum of n cols
across y rows.


### Testing DbValidate

To build the test jar, first compile via ***mvn package && mvn scala:testCompile***
followed by running the script ***src/test/resources/build-test-jar.sh***.

The test for DBValidate uses a mysql instance to run the comparison. The
following will seed the test data in both MySQL and Hive for running the test app.
```
  $ mysql -u root -p < src/test/resources/sht-mysql-init.sql
  $ hadoop fs -put src/test/resources/sht_data1.csv
  $ hadoop fs -put src/test/resources/sht_data2.csv
  $ ./src/test/resources/dbval-init.sh mysqlhost:port  
```

Run the *dbval-test.sh* script providing the hostname of the mysql server to
run the test.
```
  ./src/test/resources/dbval-test.sh localhost:3306
```

Note that scripts should be run relative to the project root directory.


## HiveTableMeta

Creates a key/value file of *table_name* to the db *create table* statement.
This can be useful to create a backup of the table metadata. The resulting 
file can be used to recreate table metadata in a different environment. This 
can be useful for cloud environments, for instance, in Azure, using ADLS 
endpoints for external tables would need their endpoint HDFS locations 
modified when tables are copied elsewhere. Note that CDH BDR(distcp) does 
not support ADLS source/targets (as of CDH5/6.0?).


## DBTableLocations

A good Hive warehouse practice is to locate all tables under a common db path.
This should be true whether or not tables are marked as `external` (unmanaged).
This app simply compares table locations with db locations and prints any
mismatches in location where a table sits physically out of the parent
database path.


# Installing

Building the project creates the jar *package* via `mvn package`.

For simple use, the package can be added to the local Maven repository
by using the `install-file` plugin.
```
mvn install:install-file -Dpackaging=jar -DgroupId=com.trace3.spark.hive \
 -DartifactId=spark-hive-tools -Dversion=0.5.0 \
 -Dfile=target/spark-hive-tools-0.5.0.jar
```

The project has a GitHub-based Maven Repository, which would need an entry 
to either maven settings or the project pom. Currently, GitHub requires 
authentication for its [Packages](https://docs.github.com/en/packages) project.
```xml
  <repositories>
      <repository>
          <id>spark-hbase-client</id>
          <url>https://maven.pkg.github.com/tcarland/spark-hive-tools</url>
      </repository>
  </repositories>
```

Maven Artifact:
```xml
  <properties>
      <scala.binary.version>2.13</scala.binary.version>
      <scala.version>2.13.5</scala.version>
  </properties>
  <dependency>
    <groupId>com.trace3.spark.hive</groupId>
    <artifactId>spark-hive-tools</artifactId>
    <version>0.5.0_${scala.binary.version}</version>
  </dependency>
```
