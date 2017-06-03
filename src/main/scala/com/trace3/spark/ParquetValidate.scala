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
    import spark.implicits._

    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition",  "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode",  "nonstrict")

    val crstr    = spark.sql("SHOW CREATE TABLE " + table).first.get(0).toString
    val pathstr  = HiveFunctions.GetTableLocation(crstr)
    val conf     = new Configuration()
    val path     = new Path(pathstr)
    val fs       = FileSystem.get(conf)
    val cols     = spark.read.parquet(path.toString).columns.map(s => s.toUpperCase)
    val files    = fs.listStatus(path).map(_.getPath.toUri).filter(!_.toString.contains("_SUCCESS"))

    println("Table Location: " + pathstr)
    println("Table Columns:")
    cols.foreach(s => println("  " + s))

    files.foreach {
      uri => {
        println("foreach uri: " + uri.toString)
        val colp = spark.read.parquet(uri.toString).columns.map(s => s.toUpperCase)

        //println("Partition: " + uri.toString)
        colp.foreach(s => println("  " + s))

        println("Columns in MAIN missing in Partition:")
        cols.diff(colp).foreach(println(_))
      }
    }



    spark.stop
  }
}
