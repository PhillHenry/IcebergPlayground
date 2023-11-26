package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.SparkForTesting.{tmpDir, spark => testSpark}
import uk.co.odinconsultants.documentation_utils.{Datum, SimpleFixture}

import java.lang.reflect.Field


trait SimpleSparkFixture extends SimpleFixture {

  val tables = new HadoopTables(testSpark.sparkContext.hadoopConfiguration)

  val spark: SparkSession = testSpark

  def dataDir(tableName: String): String = TestUtils.dataDir(tableName)

  def icebergTable(tableName: String): Table =
    tables.load(s"$tmpDir/$tableName")

}

object TestUtils {
  def dataDir(tableName: String) = s"$tmpDir/$tableName/data"
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
    s"""CREATE TABLE $tableName ($fields)""".stripMargin
  }

  def insertSQL(tableName: String, data: Seq[Datum]): String = {
    def subquery(f: Field => String): String = classOf[Datum].getDeclaredFields
      .map { field: Field =>
        s"${f(field)}"
      }
      .mkString(",\n")
    val fields: String                       = subquery(_.getName)
    val values: Seq[String]                  = data.map((x: Datum) => s"(${x.id}, '${x.label}', ${x.partitionKey})")
    s"""INSERT INTO TABLE $tableName ($fields) VALUES ${values.mkString(",\n")}""".stripMargin
  }
}
