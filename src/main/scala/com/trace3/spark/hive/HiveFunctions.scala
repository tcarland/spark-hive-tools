/** HiveFunctions.scala
  *
  * @author Timothy C. Arland <tcarland@gmail.com>
  *
  */
package com.trace3.spark.hive

import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex


// note: some of these are a little ugly and special case
// early impl needs continued and improved abstraction

/** HiveFunctions
  *
  * A collection of functions for interacting with Hive and underlying Parquet tables.
  *
 **/
object HiveFunctions {


  /** Given a fully qualified table name (ie. schema.table),
    * return the schema name only (minus the table name).
   **/
  def GetDBName ( fqtn: String ) : Option[String] = {
    val dbpat = """(\S+)\.\S+""".r
    fqtn match {
      case dbpat(dbname) => Option(dbname)
      case (_) => None
    }
  }


  /** Given a fully qualified table name (ie. schema.table),
    * return the table name only (minus the schema name).
   **/
  def GetTableName ( fqtn: String ) : String = {
    val tbpat = """\S+\.(\S+)""".r

    fqtn match {
      case tbpat(tblName) => tblName
      case (_) => fqtn
    }
  }


  /** Determines the Location URI from a given table name if the string
    * provided is not already a valid HDFS path.
    *
    * @param spark  A SparkSession context.
    * @param table  The table name string in either path or db.name format
    * @return  The path or uri to the table or an empty string if undetermined.
    *
   **/
  def GetTableURI ( spark: SparkSession, table: String ) : String = {
    var path = ""

    if ( table.contains("/") ) {
      path = table
    } else {
      val crstr = spark.sql("SHOW CREATE TABLE " + table)
        .first
        .get(0)
        .toString

      path = HiveFunctions.GetTableLocationString(crstr)

      if ( path.isEmpty ) {
        path = spark.conf.getOption("hive.metastore.warehouse.dir").getOrElse("/user/hive/warehouse")

        if ( table.contains(".") )
          path += "/" + HiveFunctions.GetDBName(table).get + ".db" + "/" + HiveFunctions.GetTableName(table)
        else
          path += table
      }
    }
    // validate hdfs path

    path
  }

  def GetCreateTableString ( spark: SparkSession, table: String ) : String = {
    val createstr = spark.sql("SHOW CREATE TABLE " + table)
      .first
      .getAs[String](0)
      .replaceAll("\n", " ")
      .replaceAll("  ", " ")
    createstr
  }


  /**  Given the full Hive SHOW CREATE TABLE string, extract the
    *  table location. Useful for determining the HDFS location of
    *  an external table.
    * @param createStr  The Hive CREATE TABLE string
    * @return The LOCATION target string
    */
  def GetTableLocationString ( createStr: String ) : String = {
    val pat  = """CREATE .*TABLE.*LOCATION\s+'(.+)' TBL.*""".r

    // extract and rename location
    if ( createStr.contains("LOCATION") ) {
      val loc = createStr match {
        case pat(m1) => m1
        case _ => s""
      }
      loc
    } else {
      s""
    }
  }


  /** Determine the Location URI of a given database
    *
    * @param spark  A SparkSession context
    * @param dbname The name of the database
    * @return  The location URI of the database
    */
  def GetDatabaseLocationURI ( spark: SparkSession, dbname: String ) : String = {
    import spark.implicits._
    spark.catalog.listDatabases.select($"locationUri").where($"name" === dbname).first.getAs[String](0)
  }


  /**  Convenience function to rename a hive table location to a
    *  a new table name in the same database.
    *  eg. Given a location of
    *  'maprfs:////user/tca/hive/warehouse/default/table1'
    *  and a table name of 'table2' the new location will be
    *  'maprfs:////user/tca/hive/warehouse/default/table2'
    *  NOTE: that his only adjusts the last directory representing
    *  the table name.
    *
    * @param loc       The current location string.
    * @param tableName The name of the new table.
    * @return          Tne new location string.
    */
  def CopyTableLocation ( loc: String, tableName: String ) : String = {
    val uripat = """LOCATION\s+'(\S+/).+'$""".r

    val uri = loc match {
      case uripat(m1) => m1
    }

    s" LOCATION '" + uri + GetTableName(tableName) + "' "
  }


  /** Generates a new CREATE TABLE sql statement from the provided
    * CREATE TABLE sql string with a new table name. This essentially copies 
    * the table schema from a current table. This intentionally does not
    * include 'TBLPROPERTIES' for compatibility reasons and instead lets it
    * be regenerated at CREATE.
    *
    * TODO: Location is not db aware, which means copying tables from
    * a different database (and location) may result with the new table in
    * the wrong directory.
   **/
  def CopyTableCreate ( createsql: String, tableName: String ) : String = {
    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r
    val pat2 = """(CREATE .*TABLE.* )(LOCATION\s+'.+').*""".r
    val pat3 = """(CREATE .*TABLE\s+)`(.+)`(\(.*)""".r

    // extract table properties
    var (tbl, prop) = createsql match {
      case pat1(m1, m2) => (m1, m2)
    }

    // extract and rename location
    if ( tbl.contains("LOCATION") ) {
      val (ctbl, loc) = tbl match {
        case pat2(m1, m2) => (m1, m2)
      }
      val locuri = CopyTableLocation(loc, tableName)
      tbl = ctbl + locuri
    }

    // set tableName
    val (ctbl, name, rest) = tbl match {
      case pat3(m1, m2, m3) => (m1, m2, m3)
    }

    ctbl + " `" + tableName + "` " + rest
  }

}
