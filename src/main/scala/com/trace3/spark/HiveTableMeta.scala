/** HiveTableMeta.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path

import hive.HiveFunctions


/** HiveTableMeta
  *
  * Creates a dump of hive table create statements for a given database.
 **/
object HiveTableMeta {


  val usage : String =
    """
      | ==>  Usage: HiveTableMeta <action> <filename> <dbname>
      | ==>      action    = save|restore
      | ==>     filename   = Name of output file of create statements.
      | ==>      dbname    = Name of schema or database to dump.
    """.stripMargin


  def SaveTableMeta ( spark: SparkSession, dbname: String, outFile: String ) : Unit = {
    val tmpOut    = outFile + "-tmpout"
    val hconf     = spark.sparkContext.hadoopConfiguration
    val hdfs      = FileSystem.get(hconf)

    import spark.implicits._

    if ( hdfs.exists(new Path(outFile)) ) {
      System.err.println("Fatal Error: Output path already exists")
      System.exit(1)
    }

    if ( hdfs.exists(new Path(tmpOut)) ) {
      System.err.println("Fatal Error: Temp output already exists: " + tmpOut)
      System.exit(1)
    }

    val meta = HiveFunctions.GetCreateTableStrings(spark, dbname)

    meta.toSeq.toDF.write.csv(tmpOut)

    if ( FileUtil.copyMerge(hdfs, new Path(tmpOut),
                            hdfs, new Path(outFile),
                            false, hconf, null) )
    {
      hdfs.delete(new Path(tmpOut), true)
    }
  }

  /** Restore the metadata from file modifiying the hdfs uri for the correct
    * namenode or nameservice name. Note only the name should be provided.
    * eg. nn1:8020  or  'nameservice2' and not 'hdfs://nn1:8020/'
   **/
  def RestoreTableMeta ( spark: SparkSession, inFile: String, hdfsnn: String ) : Unit = {
    import spark.implicits._

    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r
    val pat2 = """(CREATE .*TABLE.* )(LOCATION\s+'.+')(.*)""".r
    val pat3 = """LOCATION 'hdfs://\S+?/(\S+)'""".r

    val schema = StructType(
      StructField("NAME", StringType, true) ::
      StructField("STMT", StringType, true) :: Nil
    )

    spark.read.schema(schema)
      .csv(inFile)
      .collect
      .foreach( row => {
          val createStr = row(1).toString
          val newstr = if ( createStr.contains("EXTERNAL") ) {
              val (tblstr, _) = createStr match {
                  case pat1(m1, m2) => (m1, m2)
              }
              val (ctbl, loc, rest) = createStr match {
                  case pat2(m1,m2,m3) => (m1, m2, m3)
              }
              val tblpath = loc match {
                  case pat3(m1) => m1
              }
              val newloc = s" LOCATION 'hdfs://" + hdfsnn + "/" + tblpath + "' "
              val nstr = ctbl + newloc + rest
              nstr
          } else {
              createStr
          }
          println(" ==> " + newstr)
          spark.sql(newstr)
      })
  }


  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 2 ) {
      System.err.println(usage)
      System.exit(1)
    }

    val action  = args(0)
    val csvfile = args(1)
    val dbname  = if ( args.length == 3 ) args(2) else s""

    val spark = SparkSession
      .builder()
      .appName("spark-hive-tools::HiveTableMeta")
      .enableHiveSupport()
      .getOrCreate
    spark.sparkContext.setLogLevel("WARN")

    if ( action.equalsIgnoreCase("save") )
      HiveTableMeta.SaveTableMeta(spark, dbname, csvfile)
    else if ( action.equalsIgnoreCase("restore") )
      HiveTableMeta.RestoreTableMeta(spark, csvfile)

    println(" => Finished.")
    spark.stop
  }
}
