package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.iceberg.SQL.{createDatumTable, insertSQL}

import scala.collection.mutable.{Set => MSet}

class CowAndMorSpec extends AnyWordSpec with GivenWhenThen {

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
      val sql: String = insertSQL(tableName, data)
      Given(s"SQL:\n$sql")
      When("we execute it")
      spark.sqlContext.sql(sql)
      files.addAll(dataFilesIn(tableName))
      assert(files.nonEmpty)
      Then(s"there are ${files.size} data files")
    }
  }

}
