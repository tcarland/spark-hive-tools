/** ParquetValidate.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import hive.HiveFunctions


/** ParquetValidate
  *
  * Compares partition schemas and reports on which partitions
  * are missing which columns.
  */
object ParquetValidate {


  val usage : String =
    """
      | ==>  Usage: ParquetValidate <table>
      | ==>      table       = Parquet table name to validate
    """.stripMargin


  /** Validate the partition schema versus the table schema for
    * a given parquet table.
   **/
  def validate ( spark: SparkSession, table: String ) : Unit = {
    val keypat  = """(.*)=.*""".r
    val pathstr = HiveFunctions.GetTableURI(spark, table)

    if ( pathstr.isEmpty ) {
      System.err.println("Unable to determine path to parquet table")
      System.exit(1)
    }

    val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(pathstr))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_"))

    val keycol = files(0).getName match {
      case keypat(m1) => m1
    }

    val cols = spark.read
      .parquet(pathstr)
      .columns
      .map(s => s.toUpperCase)
      .filter(s => ! s.equalsIgnoreCase(keycol))


    println(" => Path: " + pathstr)
    println(" => Num Partition Directories: " + files.length.toString)
    println(" => Partition Key: " + keycol)
    print(" => Table Columns: < ")
    cols.foreach(s => print(s + ", "))
    println(">\n => Partitions  <missing columns>")


    // Iterate on the Parquet Partitions and compare columns.
    files.foreach( path => {
      val colp = spark.read
        .parquet(path.toUri.toString)
        .columns.map(s => s.toUpperCase)

      print("    " + path.getName + " < ")
      cols.diff(colp).foreach(s => print(s + ", "))
      println(" >")
    })

  }


  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 1 ) {
      System.err.println(usage)
      System.exit(1)
    }

    val table = args(0)

    val spark = SparkSession
      .builder()
      .appName("spark-hive-tools::ParquetValidate")
      .enableHiveSupport()
      .getOrCreate

    ParquetValidate.validate(spark, table)

    println(" => Finished.")
    spark.stop
  }
}
