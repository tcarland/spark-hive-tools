package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import hive.HiveFunctions


/** ParquetValidate
  *
  * Compares partition schemas and reports on which partitions
  * are missing which columns.
  * Created by tcarland@gmail.com on 11/17/16
  */
object ParquetValidate {

  val usage : String =
    """
      |==>  Usage: ParquetValidate <table>
      |==>      table       = Parquet table name to validate
    """.stripMargin


  /** Validate the partition schema versus the table schema for
    * a given parquet table.
   **/
  def validate ( spark: SparkSession, table: String ) : Unit = {
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
    val keycol   = pkey match {   // TODO: throws a match error
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
  }


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

    ParquetValidate.validate(spark, table)

    println("> Finished.")
    spark.stop
  }
}
