/** HiveTableMeta.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
 **/
package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.collection.immutable.{List, Map}

import hive.HiveFunctions


/** HiveTableMeta
  *
  * Creates a dump of hive table create statements for a given database.
 **/
object HiveTableMeta {

  type OptMap   = Map[String, String]
  type OptList  = List[String]


  val metaSchema = StructType(
    StructField("NAME", StringType, true) ::
    StructField("STMT", StringType, true) :: Nil
  )

  val usage : String =
    """
      |Usage: HiveTableMeta [options] <action>
      |  --dbname <name>  : The name of the database to save or restore to.
      |  --inFile <file>  : The input csv file to use for 'savetarget' or 'restore."
      |  --outFile <file> : The output csv file for 'save' or 'savetarget'"
      |  --namenode <ns>  : A namenode or nameservice name to use as the restore target"
      |     <action>      : The action to take should be: save|savetarget|restore"
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



  def SaveTableMeta ( spark: SparkSession, optMap: OptMap ) : Unit = {
    import spark.implicits._

    val dbname  = optMap.getOrElse("dbname", "")
    val outFile = optMap.getOrElse("outFile", "")
    val tmpOut    = outFile + "-tmpout"
    val hconf     = spark.sparkContext.hadoopConfiguration
    val hdfs      = FileSystem.get(hconf)


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
  def SaveTargetTableMeta ( spark: SparkSession, optMap: OptMap ) : Unit = {
    import spark.implicits._

    val inFile  = optMap.getOrElse("inFile", "")
    val outFile = optMap.getOrElse("outFile", "")
    val hdfsnn  = optMap.getOrElse("namenode", "")

    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r
    val pat2 = """(CREATE .*TABLE.* )(LOCATION\s+'.+')(.*)""".r
    val pat3 = """LOCATION 'hdfs://\S+?/(\S+)'""".r


    if ( inFile.isEmpty || outFile.isEmpty || hdfsnn.isEmpty ) {
      System.err.println(" ==> Error, invalid or missing options")
      System.err.println(usage)
      System.exit(1)
    }

    val meta : Array[(String, String)] = spark.read.schema(metaSchema)
      .csv(inFile)
      .collect
      .map( row => {
          val createStr = row(1).toString
          val newstr = if ( createStr.contains("EXTERNAL") ) {
              val (tblstr, _) = createStr match {
                  case pat1(m1, m2) => (m1, m2)
              }
              val (ctbl, loc, rest) = tblstr match {
                  case pat2(m1,m2,m3) => (m1, m2, m3)
              }
              val tblpath = loc match {
                  case pat3(m1) => m1
              }
              val newloc = s" LOCATION 'hdfs://" + hdfsnn + "/" + tblpath + "' "
              ( ctbl + newloc + rest )
          } else {
              createStr
          }
          ( row(0).toString, newstr )
      })

    val tmpOut    = outFile + "-tmpout"
    val hconf     = spark.sparkContext.hadoopConfiguration
    val hdfs      = FileSystem.get(hconf)

    meta.toSeq.toDF.write.csv(tmpOut)

    if ( FileUtil.copyMerge(hdfs, new Path(tmpOut),
                            hdfs, new Path(outFile),
                            false, hconf, null) )
    {
      hdfs.delete(new Path(tmpOut), true)
    }
  }


  /** Restores table metadata from a provided meta file */
  def RestoreTableMeta ( spark: SparkSession, optMap: OptMap ) : Unit = {
    val inFile = optMap.getOrElse("inFile", "")

    spark.read.schema(metaSchema)
      .csv(inFile)
      .collect
      .foreach( row => {
        val createStr = row(1).toString
        spark.sql(createStr)
    })
  }



  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 2 ) {
      System.err.println(usage)
      System.exit(1)
    }

    val (optMap, optList) = HiveTableMeta.parseOpts(args.toList)

    if ( optList.isEmpty ) {
        System.err.println(usage)
        System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("spark-hive-tools::HiveTableMeta")
      .enableHiveSupport()
      .getOrCreate
    spark.sparkContext.setLogLevel("WARN")

    val action = optList.last

    if ( action.equalsIgnoreCase("save") )
      HiveTableMeta.SaveTableMeta(spark, optMap)
    else if ( action.equalsIgnoreCase("savetarget") )
      HiveTableMeta.SaveTargetTableMeta(spark, optMap)
    else if ( action.equalsIgnoreCase("restore") )
      HiveTableMeta.RestoreTableMeta(spark, optMap)
    else
      System.err.println("No action recognized.")

    println(" => Finished.")
    spark.stop
  }
}
