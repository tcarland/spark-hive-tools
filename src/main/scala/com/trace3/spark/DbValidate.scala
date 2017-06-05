package com.trace3.spark

import org.apache.spark.sql.SparkSession

import scala.collection.immutable.List
import scala.collection.immutable.Map


/**
  * Created by tca on 6/5/17.
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
      |  --hive-table <db.table>   : Name of the Hive table to compare against
      |  --username <user>         : The external database user
      |  --password <pw>           : Clear text password of external db user
      |                              (use --password-file is preferred
      |  --password-file <pwfile>  : Name of a user readable file containing the password
      |                              Use a fully qualified path
      |  --driver <jdbc_driver>    : The JDBC Driver class eg. 'oracle.jdbc.driver.OracleDriver'
      |  --col-compare <col1,colN> : A comma delimited list of value columns to compare
      |                              by performing a SUM(col1,col2,col3) on the 5th row of
      |                              each table (external and hive).
      |
    """.stripMargin


  def main ( args: Array[String] ) : Unit = {
    val spark = SparkSession.builder()
      .appName("spark-hive-tools::DbValidate")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val (optMap, optList) = parseOpts(args.toList)

    val jdbc    = optMap.get("jdbc")
    val driver  = optMap.get("driver")
    val dbtable = optMap.get("dbtable")
    val hvtable = optMap.get("hive-table")
    val user    = optMap.get("user")
    val pass    = optMap.get("password")
    var pwfile  = optMap.get("password-file")
    val ccols   = optMap.get("col-compare")

    if ( jdbc.isEmpty || dbtable.isEmpty ) {
      System.err.println(usage)
      System.exit(1)
    }

    var password = ""
    if ( pass.isEmpty ) {
      if ( pwfile.isEmpty ) {
        System.err.println(usage)
        System.exit(1)
      }
      var pfile = pwfile.get
      if ( ! pfile.startsWith("/") )
        pfile = "file:///" + pfile
      else
        pfile = "file://" + pfile

      password = spark.sparkContext.textFile(pfile).collect.head
    } else {
      password = pass.get
    }

    val props = new java.util.Properties
    props.setProperty("user", user.get)
    props.setProperty("password", password)
    props.setProperty("driver", driver.get)

    val edf  = spark.read.jdbc(jdbc.get, dbtable.get, props)
    val ecnt = edf.count
    val ecol = edf.columns

    val efiv = edf.take(5) // gives us an Array[Row]

    if ( ! ccols.isEmpty ) {
       // sum our columns
    }

    // hive table
    val hdf  = spark.read.table(hvtable.get)
    val hcnt = hdf.count


  } // main

} // object DbValidate


