package com.trace3.spark.hive

import scala.util.matching.Regex


object HiveFunctions {


  /** Given a fully qualified table name (ie. schema.table),
   *  return the schema name only (minus the table name).
   **/
  def GetDBName ( fqtn: String ) : Option[String] = {
    val dbpat = """(\S+)\.\S+""".r
    fqtn match {
      case dbpat(dbname) => Option(dbname)
      case (_) => None
    }
  }


  /** Given a fully qualified table name (ie. schema.table),
   *  return the table name only (minus the schema name).
   **/
  def GetTableName ( fqtn : String ) : String = {
    val tbpat = """\S+\.(\S+)""".r
    fqtn match {
      case tbpat(tblName) => tblName
      case (_) => fqtn
    }
  }


  /**  Convenience function to create a new location string from
    *  an existing table in the same destination for a new table.
    *  eg. Given a location of 
    *  'maprfs:////user/tca/hive/warehouse/default/table1'
    *  and a table name of 'table2' the new location will be
    *  'maprfs:////user/tca/hive/warehouse/default/table2'
    *  NOTE: that his only adjusts the final directory. 
    *
    * @param loc       The current location string.
    * @param tableName The name of the new table.
    * @return          Tne new location string.
    */
  def CopyTableLocation ( loc : String, tableName : String ) : String = {
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
  def CopyTableCreate ( sql : String, tableName : String ) : String = {
    val pat1 = """(CREATE .*)( TBLPROPERTIES .*)""".r
    val pat2 = """(CREATE .*TABLE.* )(LOCATION\s+'.+').*""".r
    val pat3 = """(CREATE .*TABLE\s+)`(.+)`(\(.*)""".r

    // extract table properties
    var (tbl, prop) = sql match {
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
