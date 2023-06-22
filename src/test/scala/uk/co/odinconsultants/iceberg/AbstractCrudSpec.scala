package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.iceberg.SQL.{createDatumTable, insertSQL}

import scala.collection.mutable.{Set => MSet}

abstract class AbstractCrudSpec extends AnyWordSpec with GivenWhenThen {

  import spark.implicits._

  def tableName: String
  def mode: String

  s"A $mode table" should {
    val files: MSet[String]                              = MSet.empty[String]
    val createSQL: String                                = tableDDL(tableName, mode)
    s"create no new files for $mode" in new SimpleFixture {
      Given(s"SQL:\n${Console.BLUE}'$createSQL")
      When("we execute it")
      spark.sqlContext.sql(createSQL)
      val table: Table = icebergTable(tableName)
      assert(table != null)
      Then(s"there is an Iceberg table, $table")
    }
    s"insert creates new files for $mode" in new SimpleFixture {
      val sql: String           = insertSQL(tableName, data)
      val original: Set[String] = files.toSet
      Given(s"SQL:\n$sql")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(dataFilesIn(tableName))
      assert(files.nonEmpty)
      thenTheDataFilesAre(original)
      val output: Array[Datum]  = andTheTableContains(tableName)
      assert(output.toSet == data.toSet)
    }
    s"update creates no new files for $mode" in new SimpleFixture {
      val original: Set[String] = files.toSet
      val toUpdate: Datum       = data.head
      val fileCount: Int        = dataFilesIn(tableName).length
      val sql: String           = s"UPDATE $tableName SET label='${toUpdate.label}X' WHERE id=${toUpdate.id}"
      Given(s"SQL:\n$sql")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(dataFilesIn(tableName))
      thenTheDataFilesAre(original)
      val output: Array[Datum]  = andTheTableContains(tableName)
      assert(output.toSet != data.toSet)
    }
    def thenTheDataFilesAre(previous: Set[String]): Unit = {
      val dir: String            = TestUtils.dataDir(tableName)
      val dataFiles: Seq[String] =
        files.toList.sorted.map { (x: String) =>
          val edited: String = x.substring(dir.length)
          if (previous.contains(x)) { s"${Console.GREEN}$edited" }
          else s"${Console.BLUE}$edited"
        }
      Then(s"there are now ${files.size} data files:\n${dataFiles.mkString("\n")}")
    }
  }

  private def tableDDL(tableName: String, mode: String) = {
    val createSQL: String = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.delete.mode'='$mode',
                               |    'write.update.mode'='$mode',
                               |    'write.merge.mode'='$mode'
                               |) PARTITIONED BY (${classOf[
                                Datum
                              ].getDeclaredFields.head.getName}); """.stripMargin
    createSQL
  }

  def andTheTableContains(tableName: String): Array[Datum] = {
    val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
    val rows: Array[Datum]    = table.collect()
    And(s"the table contains:\n${rows.mkString("\n")}")
    rows
  }
}
