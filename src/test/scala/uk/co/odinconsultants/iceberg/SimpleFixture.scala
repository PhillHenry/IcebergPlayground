package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.spark.sql.Dataset
import uk.co.odinconsultants.SparkForTesting._

import java.lang.reflect.Field
import java.nio.file.{Path, Paths}
import scala.annotation.tailrec

case class Datum(id: Int, label: String, partitionKey: Long)

trait Fixture[T] {
  def data: Seq[T]
}

trait TableNameFixture {
  val tableName = this.getClass.getSimpleName.replace("$", "_")
}

trait SimpleFixture extends Fixture[Datum] {

  val num_partitions = 5

  val num_rows = 20

  val tables = new HadoopTables(spark.sparkContext.hadoopConfiguration)

  val data: Seq[Datum] = Seq.range(0, num_rows).map((i: Int) => Datum(i, s"label_$i", i % num_partitions))

  def assertDataIn(tableName: String) = {
    import spark.implicits._
    val output: Dataset[Datum] = spark.read.table(tableName).as[Datum]
    assert(output.collect().toSet == data.toSet)
  }

  def dataFilesIn(tableName: String): List[String] = {
    val dir: String = TestUtils.dataDir(tableName)
    @tailrec
    def recursiveSearch(acc: Seq[String], paths: Seq[Path]): Seq[String] =
      if (paths.isEmpty) {
        acc
      } else {
        val current: Path      = paths.head
        val rest   : Seq[Path] = paths.tail
        if (current.toFile.isDirectory) {
          recursiveSearch(acc, rest ++ current.toFile.listFiles().map(_.toPath))
        } else {
          recursiveSearch(acc :+ current.toString, rest)
        }
      }
    recursiveSearch(Seq.empty[String], Seq(Paths.get(dir))).toList
  }

  def icebergTable(tableName: String): Table =
    tables.load(s"$tmpDir/$tableName")

}

object TestUtils{
  def dataDir(tableName: String) = s"$tmpDir/$tableName/data"
}

trait UniqueTableFixture extends SimpleFixture {

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
