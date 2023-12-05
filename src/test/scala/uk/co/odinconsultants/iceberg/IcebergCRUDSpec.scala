package uk.co.odinconsultants.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

import scala.collection.mutable.{Set => MSet}

class IcebergCRUDSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "A dataset we CRUD" should {
    import spark.implicits._
    val files: MSet[String] = MSet.empty[String]

    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      Then("reading the table back yields the same data")
      assertDataIn(tableName)
      files.addAll(dataFilesIn(tableName))
    }

    val newVal    = "ipse locum"
    val updateSql = s"update $tableName set label='$newVal'"
    s"support updates with '$updateSql'" in new SimpleSparkFixture {
      Given(s"SQL ${formatSQL(updateSql)}")
      When("we execute it")
      spark.sqlContext.sql(updateSql)
      Then("all rows are updated")
      val output: Dataset[Datum]  = spark.read.table(tableName).as[Datum]
      val rows: Array[Datum]      = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      assert(rows.length == data.length)
      for {
        row <- rows
      } yield assert(row.label == newVal)
      val dataFiles: List[String] = dataFilesIn(tableName)
      assert(dataFiles.length > files.size)
      files.addAll(dataFiles)
    }

    val newColumn  = "new_string"
    val alterTable =
      s"ALTER TABLE $tableName ADD COLUMNS ($newColumn string comment '$newColumn docs')"
    s"be able to have its schema updated" in {
      Given(s"SQL ${formatSQL(alterTable)}")
      When("we execute it")
      spark.sqlContext.sql(alterTable)
      Then("all rows are updated")
      val output: DataFrame = spark.read.table(tableName)
      val rows: Array[Row]  = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      for {
        row <- rows
      } yield assert(row.getAs[String](newColumn) == null)
    }

    "be able to have rows removed" in new SimpleSparkFixture {
      private val minID: Int = num_rows / 2
      val deleteSql = s"DELETE FROM $tableName where id < $minID"
      Given(s"SQL ${formatSQL(deleteSql)}")
      When("we execute it")
      spark.sqlContext.sql(deleteSql)
      Then("the rows really have gone")
      assert(spark.sqlContext.sql(s"SELECT COUNT(*) FROM $tableName where id < $minID").collect()(0).getLong(0) == 0)
    }

    "when vacuumed, have old files removed" in new SimpleSparkFixture {
      val table: Table = icebergTable(tableName)
      val filesBefore = dataFilesIn(tableName).toSet
      Given(s"the ${filesBefore.size} files are:\n${toHumanReadable(filesBefore)}")
      table
        .expireSnapshots()
        .expireOlderThan(System.currentTimeMillis())
        .commit()
      When("we call expireSnapshot")
      table.expireSnapshots()
      SparkActions
        .get()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("label", newVal))
        .option("target-file-size-bytes", (500 * 1024 * 1024L).toString) // 500 MB
        .execute()
      val afterBefore = dataFilesIn(tableName).toSet
      Then(s"the original ${filesBefore.size} files now look like:\n${toHumanReadable(afterBefore)}")
      And(s"there are ${(filesBefore -- afterBefore).size} new files")
      assert(dataFilesIn(tableName).length < files.size)
    }
  }
}
