package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.{createDatumTable, insertSQL}

import scala.collection.mutable.{Set => MSet}

abstract class AbstractCrudSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  def mode: String

  s"A $mode table" should {
    val files: MSet[String]                              = MSet.empty[String]
    val createSQL: String                                = tableDDL(tableName, mode)
    spark.sql(s"DROP TABLE  IF EXISTS $tableName  PURGE")
    s"create no new files for $mode" in new SimpleSparkFixture {
      Given(s"SQL:${formatSQL(createSQL)}")
      When("we execute it")
      spark.sqlContext.sql(createSQL)
      val table: Table = icebergTable(tableName)
      assert(table != null)
      Then(s"there is an Iceberg table, $table")
    }
    s"insert creates new files for $mode" in new SimpleSparkFixture {
      val sql: String           = insertSQL(tableName, data)
      val original: Set[String] = files.toSet
      Given(s"SQL:${formatSQL(sql)}")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(parquetFiles(tableName))
      assert(files.nonEmpty)
      val dataFiles: Seq[String] =thenTheDataFilesAre(original)
//      assert(dataFiles.size == num_partitions * 2) // *2 for CRC files
      val output: Array[Datum]  = andTheTableContains(tableName)
      assert(output.toSet == data.toSet)
    }
    s"update creates no new files for $mode" in new SimpleSparkFixture {
      val original: Set[String] = files.toSet
      val toUpdate: Datum       = data.tail.head
      val updated: Datum        = toUpdate.copy(label = s"${toUpdate.label}X")
      val sql: String           = s"UPDATE $tableName SET label='${updated.label}' WHERE id=${toUpdate.id}"
      Given(s"SQL:${formatSQL(sql)}")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(parquetFiles(tableName))
      thenTheDataFilesAre(original)
      val output: Array[Datum]  = andTheTableContains(tableName)
      assert(output.toSet != data.toSet)
      checkDatafiles(original, files.toSet, Set(updated))
    }
    s"reading an updated table using $mode" in new SimpleSparkFixture {
      Given("a table that has been updated")
      val original: Set[String] = files.toSet
      When("we read from it")
      val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      Then(s"the table still contains ${table.count()} records")
      And("there are no new data files")
      assert(parquetFiles(tableName).toSet == original)
    }
    def thenTheDataFilesAre(previous: Set[String]): Seq[String] = {
      val dataFiles: Seq[String] =
        files.toList.sorted.map { (x: String) =>
          val edited: String = simpleFileName(x)
          if (previous.contains(x)) { s"${Console.GREEN}$edited" }
          else s"${Console.CYAN}$edited"
        }
      Then(s"there are now ${files.size} data files:\n${dataFiles.mkString("\n")}${Console.RESET}")
      val deleted: Seq[String] =
        previous.toList
        .filterNot(files.contains)
        .sorted
        .map(simpleFileName)
      if (deleted.nonEmpty) {
        And(s"the deleted files are:\n${Console.RED}${deleted.mkString("\n")}${Console.RESET}")
      }
      dataFiles
    }
  }

  def simpleFileName(x: String): String = {
    val dir: String    = TestUtils.dataDir(tableName)
    x.substring(dir.length)
  }

  def checkDatafiles(previous: Set[String], current: Set[String], changes: Set[Datum]): Unit

  private def tableDDL(tableName: String, mode: String): String = {
    val createSQL: String = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.delete.mode'='$mode',
                               |    'write.update.mode'='$mode',
                               |    'write.merge.mode'='$mode'
                               |) PARTITIONED BY (${classOf[
                                Datum
                              ].getDeclaredFields.filter(_.getName.toLowerCase.contains("partition")).head.getName}); """.stripMargin
    createSQL
  }

  def andTheTableContains(tableName: String): Array[Datum] = {
    val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
    val rows: Array[Datum]    = table.collect()
    And(s"the table contains:\n${toHumanReadable(rows.sortBy(_.id))}")
    rows
  }

}
