/** DbValidate.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
  *
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.immutable.List
import scala.collection.immutable.Map

import java.sql.Timestamp
import java.text.SimpleDateFormat

import hive.HiveFunctions


/**  DbValidate : Attempts to compare an external database to a parquet|hive table
  *  by comparing the column names and optionally run a SUM(col1, col2, ..)
  *  function on '--num-rows', per partition for '--num-partitions'
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
      |  --user <user>             : The external database username.
      |  --password <pw>           : Password of the external db user.
      |                              (use of --password-file is preferred and more secure)
      |  --password-file <pwfile>  : Fully qualified path to a file containing the password.
      |  --driver <jdbc_driver>    : The JDBC Driver class eg. 'oracle.jdbc.driver.OracleDriver'
      |  --sumcols <col1,colN>     : A comma delimited list of value columns to compare
      |                              by performing a SUM(col1,col2,col3) on the 5th row of
      |                              each table (external and hive).
      |  --num-rows <n>            : The number of rows to run the SUM(columns) comparison.
      |  --num-partitions <n>      : Number of table partitions to iterate through.
      |                              Use the '-R' option to reverse the partition order and
      |                              operate on the last <n> partitions.
      |
      |   If Additional keys are needed to obtain unique rows for a column compare, the
      |   system environment variable SHT_COMPOSITE_KEY can be set to a string that will
      |   be appended to the where clause of the sql query, and should conform to the sql
      |   syntax of 'Col1=val1 AND Col2=val2 AND Col3=val3'.
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
  def pushdownQuery ( key:    StructField,
                      keyval: String,
                      addkey: String,
                      table:  String,
                      cols:   Array[String] ) : String = {
    var sql = "(SELECT " + key.name

    if ( ! cols.isEmpty )
      cols.foreach(str => sql += (", " + str))
    sql += (" FROM " + table + " WHERE " + key.name + " = ")

    key.dataType match {
      case StringType    => sql += ("\"" + keyval + "\"")
      case TimestampType => {
        var str  = keyval
        val form = "yyyy-MM-dd HH:mm:ss.S"
        if ( ! keyval.contains(":") )
          str = new Timestamp(new SimpleDateFormat(form).parse(keyval).getTime).toString
        sql += " TIMESTAMP '" + str + "'"
      }
      case _ => sql += keyval
    }

    if (  addkey.length > 1 )
      sql += addkey

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
    val sumcols = optMap.getOrElse("sumcols", "").split(',').filter(s => !s.isEmpty)
    val nparts  = optMap.getOrElse("num-partitions", "5").toInt
    val nrows   = optMap.getOrElse("num-rows", "5").toInt
    val addkey  = sys.env.getOrElse("SHT_COMPOSITE_KEY", "")
    val keypat  = """(.*)=(.*)$""".r

    if ( url.isEmpty || dbtable.isEmpty || dbkey.isEmpty ) {
      System.err.println(usage)
      System.exit(1)
    }

    val password : String = if ( pass.isEmpty ) {
      if ( pwfile.isEmpty ) {
        System.err.println("validate() Error: No Db Password found")
        System.err.println(usage)
        System.exit(1)
      }

      val pfile = if ( pwfile.startsWith("/") )
        s"file://" + pwfile
      else
        s"file:///" + pwfile

      spark.sparkContext.textFile(pfile).collect.head
    } else {
      pass
    }

    val props = new java.util.Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", driver)

    // compare top-level schema
    val extDF    = spark.read.jdbc(url, dbtable, props)
    val dbcols   = extDF.columns.map(s => s.toUpperCase)
    val hvDF     = spark.read.table(hvtable)
    val hvcols   = hvDF.columns.map(s => s.toUpperCase)

    print("\ndbtable: " + dbtable + " <")
    dbcols.foreach(s => print(s + ", "))
    println(">")
    print("Missing columns < ")
    dbcols.diff(hvcols).foreach(s => print(s + ", "))
    println(">")

    if ( sumcols.length <= 1 ) {
      println("No columns provided. Exiting early")
      return
    }

    val f : Array[Path] = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(HiveFunctions.GetTableURI(spark, hvtable)))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_"))

    val files = if ( optList.contains("R") ) {
      val (fx, _) = f.reverse.splitAt(nparts)
      fx
    } else {
      val (fx, _) = f.reverse.splitAt(nparts)
      fx
    }

    files.foreach( path => {
      val (keycol, keyval) = path.getName match {
        case keypat(m1, m2) => (m1, m2)
      }

      val sql   = pushdownQuery(extDF.schema(dbkey), keyval, addkey, dbtable, sumcols)
      val dcols = sumcols :+ dbkey

      val dbdf  = spark.read.jdbc(url, sql, props).select(dcols.head, dcols.tail: _*)
      val dbcnt = dbdf.count
      val dbsum = dbdf.withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS")).limit(nrows)

      val pqdf  = spark.read.parquet(path.toUri.toString).select(dcols.head, dcols.tail: _*)
      val pqcnt = pqdf.count
      val pqsum = pqdf.withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS")).limit(nrows)

      //println(" sql query: " + sql)
      println("\nPartition: " + keycol + "=" + keyval)
      println("External:  Count = " + dbcnt.toString)
      dbsum.show
      println("Parquet:  Count = " + pqcnt.toString)
      pqsum.show

    })

    return
  }


  def main ( args: Array[String] ) : Unit = {

    val spark = SparkSession.builder()
      .appName("spark-hive-tools::DbValidate")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val (optMap, optList) = parseOpts(args.toList)

    DbValidate.validate(spark, optMap, optList)

    println(" => Finished.")
    spark.stop
  } // main

} // object DbValidate


