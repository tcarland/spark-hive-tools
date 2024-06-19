/** HiveTableSwapper.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
 **/
package com.trace3.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.matching.Regex

import com.trace3.spark.hive.HiveFunctions


/**  The HiveTableSwapper is intended to move an existing Hive table to a new table with
  *  an optional repartition included.  This can be used to swap a new table in place
  *  of an old table and/or allowing for a table repartition using Spark's default
  *  HashPartitioner.
  *
  *  Tables imported via sqoop with the 'split-by' option on a non-sequential
  *  column will result in unbalanced partitions, so this tool can be useful for such
  *  occasions.
  *
  * HiveTableSwapper by tcarland@gmail.com on 11/17/2016
 **/
object HiveTableSwapper {


  val tableSuffix = "_tmphts"


  val usage : String =
    """
      |==>  Usage: HiveTableSwapper <srcTable> <dstTable> [n_partitions] [partition_by]
      |==>     srcTable      =  Source Hive Table to Alter
      |==>     dstTable      =  Name of the new table
      |                         (Note any existing table by this name is DROPPED!)
      |==>     n_partitions  =  Optional number of partitions for the new table.
      |==>     partition_by  =  Optional name of the column to repartition by.
    """.stripMargin



  def SwapTable ( spark: SparkSession,
                  srcTable: String,
                  dstTable: String,
                  np: Int,
                  col: String ) : Unit =
  {
    var srctbl = srcTable
    var srcdf  = spark.read.table(srcTable)
    val curnp  = srcdf.rdd.partitions.size

    // IF a repartition is needed, we use a temp table
    if ( np > 0 && curnp != np ) {
      if ( col != null )   // repartition
        srcdf = srcdf.repartition(np, srcdf(col))
      else
        srcdf = srcdf.repartition(np)

      val srcsql = HiveFunctions.GetCreateTableString(spark, srcTable)
      val tmptbl = srcTable + tableSuffix
      val tmpsql = HiveFunctions.CopyTableCreate(srcsql, tmptbl, false)

      // create the temp table, insert, and remap the source table
      spark.sql(tmpsql)
      srcdf.write.format("parquet").insertInto(tmptbl)
      spark.sql("DROP TABLE " + srcTable)
      srctbl = tmptbl
    }

    try {
      spark.sql(s"DROP TABLE IF EXISTS $dstTable")
    } catch {
      case _ : Throwable => 
    }

    spark.sql(s"ALTER TABLE $srctbl RENAME TO $dstTable")
  }


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
      .appName("spark-hive-tools::HiveTableSwapper")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition",  "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode",  "nonstrict")

    SwapTable(spark, src, dst, np, splt)

    spark.stop()
  }
}
