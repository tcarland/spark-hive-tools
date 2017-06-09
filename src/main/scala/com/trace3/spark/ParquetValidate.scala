/** ParquetValidate.scala
  * Created by tcarland@gmail.com on 11/17/16
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
    var pathstr = HiveFunctions.GetTableURI(spark, table)

    if ( pathstr.isEmpty ) {
      System.err.println("Unable to determine path to parquet table")
      System.exit(1)
    }

    val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(pathstr))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_"))

    val cols = spark.read
      .parquet(pathstr)
      .columns
      .map(s => s.toUpperCase)

    val keypat = """(.*)=.*""".r
    val keycol = files(0).getName() match {   // TODO: throws a match error
      case keypat(m1) => m1
    }


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
      .appName("spark-hive-tools::ParquetValidate")
      .enableHiveSupport()
      .getOrCreate

    ParquetValidate.validate(spark, table)

    println(" => Finished.")
    spark.stop
  }
}
