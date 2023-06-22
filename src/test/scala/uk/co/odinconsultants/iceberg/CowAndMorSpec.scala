package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.iceberg.SQL.{createDatumTable, insertSQL}

import scala.collection.mutable.{Set => MSet}

class CowAndMorSpec extends AnyWordSpec with GivenWhenThen {
  import spark.implicits._

  "A COW table" should {

    val tableName           = "test_table"
    val files: MSet[String] = MSet.empty[String]
    val createSQL: String   = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                       |    'write.delete.mode'='copy-on-write',
                       |    'write.update.mode'='copy-on-write',
                       |    'write.merge.mode'='copy-on-write'
                       |) PARTITIONED BY (${classOf[
                                Datum
                              ].getDeclaredFields.head.getName}); """.stripMargin
    "create no new files for COW" in new SimpleFixture {
      Given(s"SQL:\n'$createSQL")
      When("we execute it")
      spark.sqlContext.sql(createSQL)
      val table: Table = icebergTable(tableName)
      assert(table != null)
      Then(s"the is an Iceberg table, $table")
    }
    "insert creates new files for COW" in new SimpleFixture {
      val sql: String          = insertSQL(tableName, data)
      Given(s"SQL:\n$sql")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(dataFilesIn(tableName))
      assert(files.nonEmpty)
      Then(s"there are ${files.size} data files")
      val output: Array[Datum] = andTheTableContains(tableName)
      assert(output.toSet == data.toSet)
    }
    "update creates new files for COW" in new SimpleFixture {
      val toUpdate: Datum      = data.head
      val fileCount: Int       = dataFilesIn(tableName).length
      val sql: String          = s"UPDATE $tableName SET label='${toUpdate.label}X' WHERE id=${toUpdate.id}"
      Given(s"SQL:\n$sql")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(dataFilesIn(tableName))
//      assert(files.size > fileCount)
      Then(s"there are now ${files.size} data files")
      val output: Array[Datum] = andTheTableContains(tableName)
      assert(output.toSet != data.toSet)
    }
  }

  def andTheTableContains(tableName: String): Array[Datum] = {
    val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
    val rows: Array[Datum]    = table.collect()
    And(s"the table contains:${rows.mkString("\n")}")
    rows
  }
}
