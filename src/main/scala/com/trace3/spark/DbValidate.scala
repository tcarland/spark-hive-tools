/** DbValidate.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
  *
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.immutable.{List, Map}

import java.sql.Timestamp
import java.text.SimpleDateFormat

import hive.HiveFunctions


/**  DbValidate : Attempts to compare an external database to a parquet|hive table
  *  by comparing the column names and optionally run a SUM(col1, col2, ..)
  *  function on '--num-rows', per partition for '--num-partitions'
 **/
object DbValidate {

  type OptMap  = Map[String, String]
  type OptList = List[String]


  val usage : String =
    """
      |DbValidate [-dh] --jdbc <jdbc uri> --dbtable <db.table> --hive-table <db.table>
      |  --jdbc <uri>              : The JDBC string for connecting to external db.
      |  --dbtable <db.table>      : Name of the source external db as 'schema.table'
      |  --dbkey <keycolumn>       : Name of db column to match partition key
      |  --hive-table <db.table>   : Name of the Hive table to compare against
      |  --user <user>             : The external database username.
      |  --password <pw>           : Password of the external db user.
      |                              (use of --password-file is preferred and more secure)
      |  --password-file <pwfile>  : HDFS path to a file containing the password.
      |  --driver <jdbc_driver>    : The JDBC Driver class eg. 'oracle.jdbc.driver.OracleDriver'
      |  --sumcols <col1,colN>     : A comma delimited list of value columns to compare
      |                              by performing a SUM(col1,col2,col3) on the first 'n' rows
      |                              of each table (external and hive). Note do not use spaces.
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
                      cols:   Array[String],
                      alias:  String = "" ) : String = {
    var sql = ""

    if ( key == null || keyval.isEmpty ) {
      sql = s"(SELECT * FROM $table"
      if ( ! addkey.isEmpty )
        sql += s" WHERE $addkey"
    } else if ( key != null ) {
      sql = s"(SELECT " + key.name
      if ( ! cols.isEmpty )
        cols.foreach(str => sql += (", " + str))
      sql += s" FROM $table WHERE $key.name = "

      key.dataType match {
        case StringType    => sql += ("\"" + keyval + "\"")
        case TimestampType => {
          var str  = keyval
          if ( keyval.contains(":") )
            str = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(keyval).getTime).toString
          else
            str = new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse(keyval).getTime).toString
          sql += s"TIMESTAMP '$str'"
        }
        case _ => sql += keyval
      }

      if ( ! addkey.isEmpty )
        sql += s" AND $addkey"
    }

    if ( ! sql.isEmpty )
      sql += s") $alias"

    sql
  }


  /** Validate an external db table with a Hive/Parquet table.
    *
    * @param spark    The SparkSession context.
    * @param optMap   A Map of configuration parameters.
    * @param optList  A list of command switches.
    */
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
    val reverse = if ( optMap.contains("R") ) true else false
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
      spark.sparkContext.textFile(pwfile).collect().head
    } else {
      pass
    }

    val props = new java.util.Properties
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", driver)

    // compare top-level schema
    val extDF   = spark.read.jdbc(url, dbtable, props)
    val dbcols  = extDF.columns.map(s => s.toUpperCase)
    val hvDF    = spark.read.table(hvtable)
    val hvcols  = hvDF.columns.map(s => s.toUpperCase)
    val dcols   = sumcols :+ dbkey

    print("\ndb table: " + dbtable + " Cols: <")
    dbcols.foreach(s => print(s + ", "))
    println(">")
    print("Missing columns < ")
    dbcols.diff(hvcols).foreach(s => print(s + ", "))
    println(">")

    if ( sumcols.length <= 1 ) {
      println("No SUM columns provided. Exiting early")
      return
    }

    val f : Array[Path] = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path(HiveFunctions.GetTableURI(spark, hvtable)))
      .map(_.getPath)
      .filter(!_.getName.startsWith("_"))

    val files = if ( reverse ) {
      val (fx, _) = f.reverse.splitAt(nparts)
      fx
    } else {
      val (fx, _) = f.splitAt(nparts)
      fx
    }

    files.foreach( path => {
      val (keycol, keyval) = path.getName match {
        case keypat(m1, m2) => (m1, m2)
      }

      // First JDBC Query with SELECT *
      val sql   = pushdownQuery(extDF.schema(dbkey), "", "", dbtable, sumcols, "dbval")
      val dbdf  = spark.read.jdbc(url, sql, props)
      val dbcnt = dbdf.count()
      dbdf.createOrReplaceTempView("extdataset")

      // Second JDBC Query to perform sums
      val sql2  = pushdownQuery(extDF.schema(dbkey), "", addkey, "extdataset", sumcols)
      val dbsum = spark.sql(sql2)
        .select(dcols.head, dcols.tail.toIndexedSeq: _*)
        .withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS"))
        .limit(nrows)

      // Read the parquet partition directly
      val pqdf  = spark.read.parquet(path.toUri.toString)
      val pqcnt = pqdf.count()
      pqdf.createOrReplaceTempView("hivedataset")

      val sql3  = pushdownQuery(null, "", addkey, "hivedataset", sumcols)
      val pqsum = spark.sql(sql3)
        .select(sumcols.head, sumcols.tail.toIndexedSeq: _*)
        .withColumn("SUM", sumcols.map(c => col(c)).reduce((c1,c2) => c1+c2).alias("SUMS"))
        .limit(nrows)

      println(" JDBC Query 1: " + sql)
      println(" JDBC QUERY 2: " + sql2)
      println(" Hive QUERY 3: " + sql3)
      println("\n Partition: " + keycol + "=" + keyval)
      println("External:  Count = " + dbcnt.toString)
      dbsum.show()
      println("Parquet:   Count = " + pqcnt.toString)
      pqsum.show()
    })
  }


  def main ( args: Array[String] ) : Unit = {
    val spark = SparkSession.builder()
      .appName("spark-hive-tools::DbValidate")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val (optMap, optList) = DbValidate.parseOpts(args.toList)

    DbValidate.validate(spark, optMap, optList)

    println(" => Finished.")
    spark.stop()
  } // main

} // object DbValidate
