/** HiveFunctions.scala
  *
  * @author Timothy C. Arland <tarland@trace3.com, tcarland@gmail.com>
  *
  */
package com.trace3.spark.hive

import org.apache.spark.sql.SparkSession


/** HiveFunctions
  *
  * A collection of functions for interacting with Hive and underlying
  * Parquet tables.
 **/
object HiveFunctions {

  /** Given a fully qualified table name, ie. 'schema.table',
    * returns the schema name only (w/o the table name).
   **/
  def GetDBName ( fqtn: String ) : String = {
    val pat = """(\S+)\.\S+""".r
    fqtn match {
      case pat(dbname) => dbname
      case (_) => fqtn
    }
  }


  /** Given a fully qualified table name, ie. 'schema.table',
    * return the table name only (w/o the schema name).
   **/
  def GetTableName ( fqtn: String ) : String = {
    val pat = """\S+\.(\S+)""".r
    fqtn match {
      case pat(tblName) => tblName
      case (_) => fqtn
    }
  }


  /** Returns a normalized string (no newlines) representing the Hive
    * CREATE TABLE statement for a given table name.
    *
    * @param spark   A SparkSession context.
    * @param table   A qualified table name
    * @return        The 'CREATE TABLE' statement
    */
  def GetCreateTableString ( spark: SparkSession, table: String ) : String = {
    val createstr = spark.sql("SHOW CREATE TABLE " + table)
      .first()
      .getAs[String]("createtab_stmt")
      .replaceAll("\n", " ")
      .replaceAll("  ", " ")

    createstr
  }


  /** Return an array of tuples consisting of the table name and the
    * corresponding SHOW CREATE TABLE statement for the tables in the
    * given database..
    *
    *  @param spark   The SparkSession context.
    *  @param dbname  The name of the database or schema to capture.
    *  @return        A string tuple of table name to create_statement
   **/
  def GetCreateTableStrings ( spark: SparkSession, dbname: String ) : Array[(String, String)] = {
    import spark.implicits._
    val locations = spark.catalog.listTables(dbname)
      .collect()
      .map( table => {
        val fqtn  = table.database + "." + table.name
        val cstr  = HiveFunctions.GetCreateTableString(spark, fqtn)
        ( table.name, cstr )
      })

    locations
  }


  /** Determines the Location URI for a given table name if the string
    * provided is not already a valid HDFS path.
    *
    * @param spark  A SparkSession context.
    * @param dbtable  The table name string in either path or db.name format
    * @return  The path or uri to the table or an empty string if undetermined.
    *
   **/
  def GetTableURI ( spark: SparkSession, dbtable: String ) : String = {
    var path = ""

    if ( dbtable.contains("/") ) {
      path = dbtable // table name is already a path
    } else {
      val cstr = HiveFunctions.GetCreateTableString(spark, dbtable)
      path     = HiveFunctions.ExtractTableLocationURI(cstr)

      if ( path.isEmpty && dbtable.contains(".") ) {
        path  = HiveFunctions.GetDatabaseLocationURI(spark, HiveFunctions.GetDBName(dbtable))
        path += "/" + HiveFunctions.GetTableName(dbtable)
      }
    }

    path
  }


  /** Determine the Location URI of a given database.
    *
    * @param spark  A SparkSession context
    * @param dbname The name of the database
    * @return  The location URI of the database
    */
  def GetDatabaseLocationURI ( spark: SparkSession, dbname: String ) : String = {
    import spark.implicits._

    spark.catalog.listDatabases()
      .select($"locationUri")
      .where($"name" === GetDBName(dbname))
      .first()
      .getAs[String]("locationUri")
  }


  /**  Given the full Hive SHOW CREATE TABLE string, extract the
    *  table location. Useful for determining the HDFS location of
    *  an external table since there is no requirement to follow the
    *  '/path/to/warehouse/schema.db/table/' semantic.
    *
    * @param createStr  The Hive CREATE TABLE string
    * @return           The LOCATION target string
    */
  def ExtractTableLocationURI ( createStr: String ) : String = {
    val pat  = """CREATE .*TABLE.*LOCATION\s+'(.+)' TBL.*""".r

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


  /**  Convenience function to rename a hive table location to a new
    *  table name in the same database. eg. Given a location of
    *  'maprfs:///user/tca/hive/warehouse/default/table1'
    *  and a table name of 'table2' the new location will be
    *  'maprfs:////user/tca/hive/warehouse/default/table2'
    *
    *  NOTE: that this only adjusts the last directory representing
    *  the table name.
    *
    * @param loc       The current location string.
    * @param tableName The new name of the table.
    * @return          Tne new location string.
    */
  def CopyTableLocation ( loc: String, tableName: String ) : String = {
    val pat = """LOCATION\s+'(\S+/).+'$""".r

    val uri = loc match {
      case pat(m1) => m1
    }

    s" LOCATION '" + uri + HiveFunctions.GetTableName(tableName) + "' "
  }


  /** Generates a new CREATE TABLE sql statement from the provided
    * CREATE TABLE sql string with a new table name. This essentially copies
    * the table schema from a current table. This 'includeProps' allows for 
    * removing 'TBLPROPERTIES' for compatibility reasons and instead lets it
    * be regenerated automatically at CREATE.
    *
    * TODO: Location is not db/path aware, which means copying tables from
    * a different schema (and location) may result with the new table in
    * the wrong directory.
   **/
  def CopyTableCreate ( createsql: String, 
                        tableName: String, 
                        includeProps: Boolean ) : String = {
    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r
    val pat2 = """(CREATE .*TABLE.* )(LOCATION\s+'.+').*""".r
    val pat3 = """(CREATE .*TABLE\s+)`(.+)`(\(.*)""".r

    // extract table properties
    var (tbl, props) = createsql match {
      case pat1(m1, m2) => (m1, m2)
    }

    // extract and rename location
    if ( tbl.contains("LOCATION") ) {
      val (ctbl, loc) = tbl match {
        case pat2(m1, m2) => (m1, m2)
      }
      val locuri = HiveFunctions.CopyTableLocation(loc, tableName)
      tbl = ctbl + locuri
    }

    // set tableName
    var (ctbl, _, rest) = tbl match {
      case pat3(m1, m2, m3) => (m1, m2, m3)
    }

    ctbl += " `" + tableName + "` " + rest
    if ( includeProps )
        ctbl += props
    ctbl
  }


  def SetClusterBy ( createStr: String, clusterBy: String ) : String = {
    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r

    val (tbl, props) = createStr match {
        case pat1(m1, m2) => (m1, m2)
    }

    tbl + " " + createStr + " " + props
  }


}
