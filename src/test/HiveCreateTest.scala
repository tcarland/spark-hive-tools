package com.trace3.spark.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import hive.HiveFunctions



/**
  * Created by tca on 12/5/16.
  */
object HiveCreateTest {

  val suffix = s"_hivecreatetest"

  val usage : String = """  ==>  Usage: HiveCreateTest [schema.tablename]""".stripMargin


  def main ( args: Array[String] ) : Unit = {
    if (args.length < 1) {
      System.err.println(usage)
      System.exit(1)
    }

    var src = args(0)

    val spark = SparkSession
      .builder()
      .appName("HiveCreateTest")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

    spark.catalog.listTables("default").show

    var srcdf  = spark.read.table(src)
    val curnp  = srcdf.rdd.partitions.size
    val dbname = HiveFunctions.GetDBName(src).getOrElse(null)

    if ( dbname != null && dbname != "default" )
      spark.catalog.listTables(dbname).show

    val srcsql = HiveFunctions.GetCreateTableString(src)

    println("  ================== ")
    println("  ==>  BEFORE: ")
    println("  ==> " + srcsql)

    val target = src + suffix

    val tmpsql = HiveFunctions.CopyTableCreate(srcsql, target)

    println("\n  ==>  AFTER: ")
    println("  ==> " + tmpsql)
    println("  ================== ")

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    val cubesDF   = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")

    squaresDF.write.parquet("data/test_table/key=1")
    cubesDF.write.parquet("data/test_table/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()

    spark.stop()
  }

}
