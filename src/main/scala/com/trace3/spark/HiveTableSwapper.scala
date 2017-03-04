package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.matching.Regex

import hive.HiveFunctions


/**
  *   The HiveTableSwapper is intended to move an existing Hive table to a new table with
  *   an optional repartition included.  This can be used to swap a new table in place
  *   of an old table and/or allowing for a table repartition (using Spark's default 
  *   HashPartitioner).
  *   Tables imported via sqoop with the 'split-by' option on a non-sequential
  *   column will result in unbalanced partitions, so this tool can be useful for such
  *   occasions.
  *
  * Created by tca on 11/17/16
  */
object HiveTableSwapper {


  val tableSuffix = "_tmphts"


  def usage() =
    """
      ==>  Usage: HiveTableSwapper <srctable> <dstTable> [num_partitions] [partition_by]
      ==>      srctable       =  Source Hive Table to Alter
      ==>      dsttable       =  Name of the new table (Note any existing table is DROPPED!)
      ==>      num_partitions =  Optional number of partitions for the new table.
      ==>      partition_by   =  Optional name of the column to repartition by.
    """.stripMargin




  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 2 ) {
      System.err.println(usage)
      System.exit(1)
    }

    var  src  = args(0)
    val  dst  = args(1)
    var  np   = 0
    var  splt = new String("")

    if ( args.length > 2 )
      np   = args(2).toInt

    if ( args.length > 3 )
      splt  = args(3)

    val spark = SparkSession
      .builder()
      .appName("HiveTableSwapper")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._

    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition",  "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode",  "nonstrict")

    var srcdf  = spark.read.table(src)
    val curnp  = srcdf.rdd.partitions.size

    // IF a repartition is needed, we create a temp table to write to
    if ( np > 0 && curnp != np ) {

      // repartition
      if ( splt != null )
        srcdf = srcdf.repartition(np, srcdf(splt))
      else
        srcdf = srcdf.repartition(np)

      // Get CREATE TABLE sql from Hive and manipulate
      val srcsql = spark.sql("SHOW CREATE TABLE " + src).first.get(0).toString.
        replaceAll("\n", " ").replaceAll("  ", " ")
      val tmptbl = src + tableSuffix
      val tmpsql = HiveFunctions.CopyTableCreate(srcsql, tmptbl)

      // create the temp table, insert, and remap the source table
      spark.sql(tmpsql)
      srcdf.write.format("parquet").insertInto(tmptbl)
      spark.sql("DROP TABLE " + src)
      src = tmptbl
    }

    try {
      spark.sql("DROP TABLE IF EXISTS " + dst)
    } catch {
      case _ : Throwable => 
    }

    spark.sql("ALTER TABLE " + src + " RENAME TO " + dst)

    spark.stop
  }
}
