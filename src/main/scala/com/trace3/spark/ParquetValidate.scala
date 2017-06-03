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

    val conf     = new Configuration()
    val fs       = FileSystem.get(conf)
    var pathstr  = ""

    if ( table.contains("/") ) {
      pathstr = table
    } else {
      val crstr  = spark.sql("SHOW CREATE TABLE " + table).first.get(0).toString
      pathstr    = HiveFunctions.GetTableLocation(crstr)

      if ( pathstr.isEmpty ) {
        pathstr = spark.conf.getOption("hive.metastore.warehouse.dir").getOrElse("/user/hive/warehouse")
        println(" ==> Hive Warehouse Dir = " + pathstr)

        if ( table.contains(".") )
          pathstr += "/" + HiveFunctions.GetDBName(table).get + ".db" +
              "/" + HiveFunctions.GetTableName(table)
        else
          pathstr += table
      }
    }
    println("  ==> Using Dir " + pathstr)

    val path     = new Path(pathstr)
    val cols     = spark.read.parquet(path.toString).columns.map(s => s.toUpperCase)
    val files    = fs.listStatus(path).map(_.getPath).filter(!_.getName.toString.contains("_SUCCESS"))

    println("Table Columns:")
    cols.foreach(s => println("  " + s))
    println("Partitions  < missing columns >")

    files.foreach {
      path => {
        // : Array[String]
        val colp = spark.read.parquet(path.toUri.toString).columns.map(s => s.toUpperCase)

        print(" ==>  " + path.getName + " < ")
        //println("Columns:")
        //colp.foreach(s => println("  " + s))

        cols.diff(colp).foreach(s => print(s + ", "))
        println(" >")
      }
    }

    spark.stop
  }
}
