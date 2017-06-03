package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.matching.Regex

import hive.HiveFunctions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
  * Created by tcarland@gmail.com on 11/17/16
  */
object ParquetValidate {


  val tableSuffix = "_tmphts"


  val usage : String =
    """
      |==>  Usage: ParquetValidate <table>
      |==>      table       = Parquet table name to validate
    """.stripMargin



  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 1 ) {
      System.err.println(usage)
      System.exit(1)
    }

    val table = args(0)

    val spark = SparkSession
      .builder()
      .appName("ParquetValidate")
      .enableHiveSupport()
      .getOrCreate

    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition",  "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode",  "nonstrict")

    val fs       = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var pathstr  = HiveFunctions.GetTableURI(spark, table)

    if ( pathstr.isEmpty ) {
      System.err.println("Unable to determine path to parquet table")
      System.exit(1)
    }

    val path     = new Path(pathstr)
    val files    = fs.listStatus(path).map(_.getPath).filter(!_.getName.startsWith("_"))
    val pkey     = files(0).getName()
    val keypat   = """(.*)=.*""".r
    val keycol   = pkey match {   //TODO: throws a match error
      case keypat(m1) => m1
    }

    val cols = spark.read
      .parquet(path.toString)
      .columns
      .map(s => s.toUpperCase)

    println(" > Using path: " + pathstr)
    println(" > Number of Partition Directories: " + files.size.toString)
    println(" > Partition Key: " + keycol)
    print(" > Table Columns: < ")
    cols.foreach(s => print(s + ", "))
    println(">\n > Partitions  <missing columns>")

    // Iterate on the Parquet Partitions and compare columns.
    files.foreach( path => {
      val colp = spark.read
        .parquet(path.toUri.toString)
        .columns.map(s => s.toUpperCase)

      print("    " + path.getName + " < ")

      // diff columns ignoring the partition key
      cols.diff(colp).foreach(s => {
        if ( ! s.equalsIgnoreCase(keycol) )
          print(s + ", ")
      })

      println(" >")
    })

    println("> Finished.")
    spark.stop
  }
}
