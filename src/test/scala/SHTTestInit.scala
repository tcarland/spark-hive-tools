/**
  * Created by tca on 6/25/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._


/* Initializes our DataSets for running a DbValidate job */
object SHTTestInit {

  def usage : String =
    """
      |  Usage: SHTTestInit <dbhost:port>
      |
    """.stripMargin

  def main ( args: Array[String] ) : Unit = {
    if ( args.length < 1 ) {
      println(usage)
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("SHTTestInit")
      .enableHiveSupport()
      .getOrCreate


    val host    = args(0)
    val user    = "sht"
    val pass    = "shttester"
    val dbname  = "sht_test"
    val driver  = "com.mysql.jdbc.Driver"
    val dbtable = dbname + ".testdata1"
    val hvtable = "default.sht_testdata2"
    val url     = "jdbc:mysql://" + host + "/sht_test?useSSL=false"

    val schema = StructType(
      StructField("FLOWID", LongType, false) ::
      StructField("DSTADDR", StringType, false) ::
      StructField("DSTPFX", StringType, false) ::
      StructField("DSTPORT", IntegerType, true) ::
      StructField("FLAGS", StringType, true) ::
      StructField("BYTES", LongType, true) ::
      StructField("PKTS", LongType, true) ::
      StructField("PROTO", IntegerType, true) ::
      StructField("SRCADDR", StringType, false) ::
      StructField("SRCPFX", StringType, false) ::
      StructField("SRCPORT", IntegerType, true) ::
      StructField("STATE", StringType, true) ::
      StructField("TIME", TimestampType, false) ::
      StructField("FLOWKEY", LongType, false) :: Nil
    )

    val props = new java.util.Properties
    props.setProperty("user", user)
    props.setProperty("password", pass)
    props.setProperty("driver", driver)

    // write to mysql table
    // we intentionally do not clear the table first to allow us to rerun
    // and increase the diff output
    val df = spark.read.schema(schema).option("header", true).csv("sht_data1.csv")
    df.write.mode(SaveMode.Append).jdbc(url, dbtable, props)

    // write to hive table
    val df2 = spark.read.schema(schema).option("header", true).csv("sht_data2.csv")
    spark.sql(s"DROP TABLE IF EXISTS $hvtable")
    spark.sql(
      s"CREATE TABLE $hvtable ()
          FLOWID BIGINT,
          DSTADDR STRING,
          DSTPFX STRING,
          DSTPORT INT,
          FLAGS STRING,
          BYTES BIGINT,
          PKTS BIGINT,
          PROTO INT,
          SRCADDR STRING,
          SRCPFX STRING,
          SRCPORT INT, 
          STATE STRING,
          TIME TIMESTAMP,
          FLOWKEY BIGINT
      ) USING parquet OPTIONS ( `serialization.format` '1' ) 
      PARTITIONED BY ( FLOWKEY )"
    )
    
    df2.write.format("parquet").mode(SaveMode.Append).insertInto(hvtable)
    spark.stop()
  }

}
