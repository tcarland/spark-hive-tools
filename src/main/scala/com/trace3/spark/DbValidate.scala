package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.List
import scala.collection.immutable.Map

import hive.HiveFunctions
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


/**
  * Created by tca on 6/1/17.
  */
object DbValidate {

  type OptMap  = Map[String, String]
  type OptList = List[String]
  val  dbalias = "dbval_alias"


  val usage : String =
    """
      |DbValidate [-dh] --jdbc <jdbc uri> --dbtable <dbtable> --hive-table <table>
      |  --jdbc <uri>              : The JDBC string for connecting to external db.
      |  --dbtable <db.table>      : Name of the source, external db schema.table
      |  --dbkey <keycolumn>       : Name of db column to match partition key
      |  --hive-table <db.table>   : Name of the Hive table to compare against
      |  --username <user>         : The external database user
      |  --password <pw>           : Clear text password of external db user
      |                              (use --password-file is preferred
      |  --password-file <pwfile>  : Name of a user readable file containing the password
      |                              Use a fully qualified path
      |  --driver <jdbc_driver>    : The JDBC Driver class eg. 'oracle.jdbc.driver.OracleDriver'
      |  --sumcols <col1,colN>     : A comma delimited list of value columns to compare
      |                              by performing a SUM(col1,col2,col3) on the 5th row of
      |                              each table (external and hive).
      |  --num-rows <n>            : Limit of number of rows to compare columns
      |  --num-partitions <n>      : Number of table partitions to iterate through
      |
    """.stripMargin



  /** Function for recursively parsing an argument list providing both a 
    * 'List' of flags (with no arguments) and a 'Map' of argument key/values
   **/
  def parseOpts ( args: OptList ) : (OptMap, OptList)  =
  {
    def nextOpt ( argList: OptList, optMap: OptMap ) : (OptMap, OptList) = {
      val longOpt = "^--(\\S+)".r
      val regOpt  = "^-(\\S+)".r

      argList match {
        case Nil => (optMap, argList)
        case longOpt(opt)  :: value  :: tail => nextOpt(tail, optMap ++ Map(opt -> value))
        case regOpt(opt)             :: tail => nextOpt(tail, optMap ++ Map(opt -> null))
        case _ => (optMap, argList)
      }
    }

    nextOpt(args, Map())
  }


  /** Constructs our query for pushdown to spark.read.jdbc  */
  def pushdownQuery ( key: StructField, keyval: String, table: String, cols: Array[String] ) : String = {
    var sql = "(SELECT " + key.name

    if ( ! cols.isEmpty )
      cols.foreach(str => sql += (", " + str))
    sql += (" FROM " + table + " WHERE " + key.name + " = ")

    key.dataType match {
      case StringType    => sql += ("\"" + keyval + "\"")
      case TimestampType => sql += "'" + keyval + "'"
      case _ => sql += keyval
    }
    sql += ") " + dbalias

    sql
  }


 
  def validate ( spark: SparkSession, optMap: OptMap, optList: OptList ) : Unit = {
    val url     = optMap.getOrElse("jdbc", "")
    val driver  = optMap.getOrElse("driver", "com.mysql.jdbc.Driver")
    val dbtable = optMap.getOrElse("dbtable", "")
    val dbkey   = optMap.getOrElse("dbkey", "")
    val hvtable = optMap.getOrElse("hive-table", "")
    val user    = optMap.getOrElse("user", "")
    val pass    = optMap.getOrElse("password", "")
    val pwfile  = optMap.getOrElse("password-file", "")
    val sumcols = optMap.getOrElse("sumcols", "").split(',')
    val nparts  = optMap.getOrElse("num-partitions", "5").toInt
    val nrows   = optMap.getOrElse("num-rows", "5").toInt
    val keypat  = """(.*)=(.*)$""".r

    if ( url.isEmpty || dbtable.isEmpty || dbkey.isEmpty ) {
      System.err.println(usage)
      System.exit(1)
    }

    var password = ""
    if ( pass.isEmpty ) {
      if ( pwfile.isEmpty ) {
        System.err.println("validate() Error: No Db Password found")
        System.err.println(usage)
        System.exit(1)
      }
      var pfile = pwfile

      if ( ! pfile.startsWith("/") )
        pfile  = "file:///" + pfile
      else
        pfile  = "file://" + pfile

      password = spark.sparkContext.textFile(pfile).collect.head
    } else {
      password = pass
    }

    val props = new java.util.Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", driver)

    // compare top-level schema
    val extDF    = spark.read.jdbc(url, dbtable, props)
    val dbcols   = extDF.columns
    val dbtype   = extDF.schema(dbkey).dataType
    val hvDF     = spark.read.table(hvtable)
    val hvcols   = hvDF.columns

    println("dbtable: " + dbtable + " <")
    dbcols.foreach(s => print(s + ", "))
    println(">")
    println("Missing columns < ")
    hvcols.diff(dbcols).foreach(s => print(s + ", "))
    println(">")


    val (files, _) = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(HiveFunctions.GetTableURI(spark, hvtable)))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_"))
      .splitAt(nparts)


    files.foreach( path => {
      val (keycol, keyval) = path.getName match {
        case keypat(m1, m2) => (m1, m2)
      }

      val sql   = pushdownQuery(extDF.schema(dbkey), keyval, dbtable, sumcols)
      val dcols = sumcols :+ dbkey

      val dbdf  = spark.read.jdbc(url, sql, props)
        .withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS"))
        .limit(nrows)

      val pqdf  = spark.read.parquet(path.toUri.toString)
        .select(dcols.head, dcols.tail: _*)
        .withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS"))
        .limit(nrows)

      println("External Database:")
      dbdf.show
      println("Parquet Table:")
      pqdf.show

      val dcnt = dbdf.count
      val pcnt = pqdf.count

      println("partition " + keycol + "=" + keyval)
      println("  external: " + dcnt.toString)
      println("  hive:     " + pcnt.toString)

    })

  }



  def main ( args: Array[String] ) : Unit = {

    val spark = SparkSession.builder()
      .appName("spark-hive-tools::DbValidate")
      .enableHiveSupport()
      .getOrCreate()

    val (optMap, optList) = parseOpts(args.toList)

    DbValidate.validate(spark, optMap, optList)

    println(" => Finished.")
    spark.stop
  } // main

} // object DbValidate


