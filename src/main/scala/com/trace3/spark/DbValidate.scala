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
      |  --columns <col1,colN>     : A comma delimited list of value columns to compare
      |                              by performing a SUM(col1,col2,col3) on the 5th row of
      |                              each table (external and hive).
      |  --num-rows <n>            : Limit of number of rows to compare columns
      |  --num-partitions <n>      : Number of table partitions to iterate through
      |
    """.stripMargin



  def validate ( spark: SparkSession, optMap: OptMap, optList: OptList ) : Unit = {
    val jdbc    = optMap("jdbc")
    val driver  = optMap("driver")
    val dbtable = optMap("dbtable")
    val dbkey   = optMap("dbkey")
    val hvtable = optMap("hive-table")
    val user    = optMap("user")
    val pass    = optMap("password")
    var pwfile  = optMap("password-file")
    val sumcols = optMap("columns").split(',')
    val nparts  = optMap.getOrElse("num-partitions", "5").toInt
    val nrows   = optMap.getOrElse("num-rows", "5").toInt

    if ( jdbc.isEmpty || dbtable.isEmpty || dbkey.isEmpty ) {
      System.err.println(usage)
      System.exit(1)
    }

    var password = ""
    if ( pass.isEmpty ) {
      if ( pwfile.isEmpty ) {
        System.err.println(usage)
        System.exit(1)
      }
      var pfile = pwfile
      if ( ! pfile.startsWith("/") )
        pfile = "file:///" + pfile
      else
        pfile = "file://" + pfile

      password = spark.sparkContext.textFile(pfile).collect.head
    } else {
      password = pass
    }

    val props = new java.util.Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", driver)

    // compare top-level schema
    val dcol = spark.read.jdbc(jdbc, dbtable, props).columns
    val hcol = spark.read.table(hvtable).columns

    println("Missing columns < ")
    hcol.diff(dcol).foreach(s => print(s + ", "))
    println(">")

    val huri = HiveFunctions.GetTableURI(spark, hvtable)
    val pat  = """(.*)=(.*)$""".r

    val (files, _) = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(huri))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_")).splitAt(nparts)

    files.foreach( path => {
      val (keycol, keyval) = path.getName match {
        case pat(m1, m2) => (m1, m2)
      }

      // keyval type consideration
      val dbdf = spark.read.jdbc(jdbc, dbtable, props)
      val pqdf = spark.read.parquet(path.toUri.toString)

      dbdf.schema(dbkey).dataType match {
        case StringType => "where "
      }

      if ( ! sumcols.isEmpty ) {
        val cols  = sumcols :+ dbkey
        val sumDF = dbdf.select(cols.head, cols.tail: _*)
          .withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS"))
          .limit(nrows)
        sumDF.show
      }
      val dcnt = dbdf.count
      val pcnt = pqdf.count

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




/*

// preparing some example data - df1 with String type and df2 with Timestamp type
val df1 = Seq(("a", "2016-02-01"), ("b", "2016-02-02")).toDF("key", "date")
val df2 = Seq(
  ("a", new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01").getTime)),
  ("b", new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-02").getTime))
).toDF("key", "date")

// If column is String, converts it to Timestamp
def normalizeDate(df: DataFrame): DataFrame = {
  df.schema("date").dataType match {
    case StringType => df.withColumn("date", unix_timestamp($"date", "yyyy-MM-dd").cast("timestamp"))
    case _ => df
  }
}

// after "normalizing", you can assume date has Timestamp type -
// both would print the same thing:
normalizeDate(df1).rdd.map(r => r.getAs[Timestamp]("date")).foreach(println)
normalizeDate(df2).rdd.map(r => r.getAs[Timestamp]("date")).foreach(println)


import java.util.Properties

// Option 1: Build the parameters into a JDBC url to pass into the DataFrame APIs
val jdbcUsername = "USER_NAME"
val jdbcPassword = "PASSWORD"
val jdbcHostname = "HOSTNAME"
val jdbcPort = 3306
val jdbcDatabase ="DATABASE"
val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

// Option 2: Create a Properties() object to hold the parameters. You can create the JDBC URL without passing in the user/password parameters directly.
val connectionProperties = new Properties()
connectionProperties.put("user", "USER_NAME")
connectionProperties.put("password", "PASSWORD")


val df2 = spark.read.jdbc(url, "table_state", props)
  .select("table_name", "target", "last_update")
  .where("last_update = 1491945095510").explain

        //val sumdf = dbdf.select(ccols.map(c => sum(c)): _*)
 */
