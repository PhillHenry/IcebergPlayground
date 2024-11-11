package uk.co.odinconsultants.iceberg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.SparkForTesting.{spark => testSpark}
import uk.co.odinconsultants.documentation_utils.{Datum, SimpleFixture}

import java.lang.reflect.Field


trait SimpleSparkFixture extends SimpleFixture {

  val hadoopConfig: Configuration = testSpark.sparkContext.hadoopConfiguration
  hadoopConfig.set(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString, "false")
  val tables = new HadoopTables(hadoopConfig)

  val spark: SparkSession = testSpark

  def dataDir(tableName: String): String = {
    if (tableName.toLowerCase.contains("polaris")) {
      s"${TestUtils.tableDir(tableName)}/data"
    } else {
      TestUtils.tableDir(tableName)
    }
  }

  def icebergTable(tableName: String): Table = {
    Spark3Util.loadIcebergTable(spark, tableName)
  }

  def parquetFiles(tableName: String): Seq[String] = super.dataFilesIn(tableName).filter(_.endsWith(".parquet"))

  def partitionField: String =
    classOf[
      Datum
    ].getDeclaredFields.filter(_.getName.toLowerCase.contains("partition")).head.getName
}

object TestUtils {
  def tableDir(tableName: String): String = s"/tmp/${tableName.replace(".", "/")}" // as defined by the external Polaris service

  def dataDir(tableName: String) = s"${tableDir(tableName)}/data"
}

trait UniqueTableFixture extends SimpleSparkFixture {

  val IntField: String = "id"

  val tableName: String = this.getClass.getName.replace("$", "_").replace(".", "_")

}

object SQL {
  def createDatumTable(tableName: String): String = {
    val fields: String = classOf[Datum].getDeclaredFields
      .map { field: Field =>
        s"${field.getName} ${field.getType.getSimpleName}"
      }
      .mkString(",\n")
    s"""CREATE TABLE $tableName ($fields) USING iceberg""".stripMargin
  }

  def insertSQL(tableName: String, data: Seq[Datum]): String = {
    def subquery(f: Field => String): String = classOf[Datum].getDeclaredFields
      .map { field: Field =>
        s"${f(field)}"
      }
      .mkString(",\n")
    val fields: String                       = subquery(_.getName)
    val values: Seq[String]                  = data.map(_.toInsertSubclause)
    s"""INSERT INTO TABLE $tableName ($fields) VALUES ${values.mkString(",\n")}""".stripMargin
  }
}
