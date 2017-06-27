/** HiveCreateTest.scala
  * Created by tca on 12/5/16.
 **/

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path

import com.trace3.spark.hive.HiveFunctions
import com.trace3.spark.ParquetValidate


object HiveCreateTblTest {

  val suffix = s"_hivecreatetest"

  val usage : String = """  ==>  Usage: HiveCreateTblTest [schema.tablename]""".stripMargin


  def main ( args: Array[String] ) : Unit = {
    if (args.length < 1) {
      System.err.println(usage)
      System.exit(1)
    }

    val src = args(0)

    val spark = SparkSession
      .builder()
      .appName("spark-hive-tools::HiveCreateTblTest")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")


    // LIST ALL TABLES EVERYWHERE!
    println("Metastore Tables:")
    spark.catalog.listDatabases
      .select($"name", $"locationUri")
      .collect
      .foreach( row => {
        val name = row.getAs[String]("name")
        if ( ! name.isEmpty ) {
          println("  " + name + "  :  " + row.getAs[String]("locationUri"))
          spark.catalog.listTables(name).show(50, false)
        }
      })

    val srcdf  = spark.read.table(src)
    val curnp  = srcdf.rdd.partitions.length
    val dbname = HiveFunctions.GetDBName(src)

    if ( dbname != null && dbname != "default" )
      spark.catalog.listTables(dbname).show

    val srcsql = HiveFunctions.GetCreateTableString(spark, src)

    println("  ================== ")
    println("  ==>  BEFORE: ")
    println("  ==> " + srcsql)

    val target = src + suffix

    val tmpsql = HiveFunctions.CopyTableCreate(srcsql, target)

    println("\n  ==>  AFTER: ")
    println("  ==> " + tmpsql)
    println("  ================== ")

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    val cubesDF   = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")

    squaresDF.write.parquet("test.db/test_table/key=1")
    cubesDF.write.parquet("test.db/test_table/key=2")

    ParquetValidate.validate(spark, "test.db/test_table")

    org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .delete(new Path("test.db"), true)


    spark.stop()
  }

}
