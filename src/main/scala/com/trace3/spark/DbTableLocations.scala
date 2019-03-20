/** DBTablesLocations.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import hive.HiveFunctions


object DbTableLocations {

  val usage : String =
    """
      | ==> Usage: DBTableLocations <dbname>
      | ==>     dbname  : Name of Hive Database
    """.stripMargin


  def ValidateTableLocations ( spark: SparkSession, dbname: String ) : Unit = {
    import spark.implicits._

    val tables = spark.catalog.listTables(dbname)
      .select($"name")
      .where($"tableType" === "EXTERNAL")
      .collect

    val dbloc  = HiveFunctions.GetDatabaseLocationURI(spark, dbname);
    println(" ==> Database Location: " + dbloc)

    tables.foreach( row => {
        val fqtn   = dbname + "." + row.getString(0)
        val tblloc = HiveFunctions.GetTableURI(spark, fqtn)
        if ( tblloc.contains(dbloc) ) {
            println(" ==> match " + fqtn)
        } else {
            println(" ==> MISMATCH: " + tblloc)
        }
    })
  }


  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 1 ) {
      System.err.println(usage)
      System.exit(1)
    }

    val dbname = args(0)

    val spark = SparkSession
      .builder()
      .appName("spark-hive-tools::ParquetValidate")
      .enableHiveSupport()
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    DbTableLocations.ValidateTableLocations(spark, dbname)

    println(" ==> Finished.")
    spark.stop
  }
}