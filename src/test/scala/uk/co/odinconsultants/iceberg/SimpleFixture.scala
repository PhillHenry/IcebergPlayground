package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import uk.co.odinconsultants.SparkForTesting._

import java.lang.reflect.Field
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors
import scala.jdk.javaapi.CollectionConverters

case class Datum(id: Int, label: String)

trait Fixture[T] {
  def data: Seq[T]
}

trait SimpleFixture extends Fixture[Datum] {

  val tables = new HadoopTables(spark.sparkContext.hadoopConfiguration)

  val data: Seq[Datum] = Seq(Datum(41, "phill"), Datum(42, "henry"))

  def dataFilesIn(tableName: String): List[String] = {
    val dir                         = s"$tmpDir/$tableName/data"
    val paths: java.util.List[Path] = Files.list(Paths.get(dir)).collect(Collectors.toList())
    CollectionConverters.asScala(paths).toList.map(_.toString)
  }

  def icebergTable(tableName: String): Table =
    tables.load(s"$tmpDir/$tableName")

}

trait UniqueTableFixture extends SimpleFixture {

  import spark.implicits._

  val IntField: String = "id"

  val tableName: String = this.getClass.getName.replace("$", "_").replace(".", "_")

}

object SQL {
  def createDatumTable(tableName: String): String = {
    val fields: String = classOf[Datum].getDeclaredFields.map { field: Field =>
      s"${field.getName} ${field.getType.getSimpleName}"
    }.mkString(",\n")
    s"""CREATE TABLE $tableName ($fields)""".stripMargin
  }
}
