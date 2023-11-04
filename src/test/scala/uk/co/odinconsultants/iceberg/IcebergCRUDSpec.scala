package uk.co.odinconsultants.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

import scala.collection.mutable.{Set => MSet}

class IcebergCRUDSpec extends AnyWordSpec with GivenWhenThen {
  "A dataset to CRUD" should {
    import spark.implicits._
    val tableName           = "spark_file_test_writeTo"
    val files: MSet[String] = MSet.empty[String]

    "create the appropriate Iceberg files" in new SimpleFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      Then("reading the table back yields the same data")
      val output: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(output.collect().toSet == data.toSet)
      files.addAll(dataFilesIn(tableName))
    }

    val newVal    = "ipse locum"
    val updateSql = s"update $tableName set label='$newVal'"
    s"support updates with '$updateSql'" in new SimpleFixture {
      Given(s"SQL '$updateSql")
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
    s"updates the schema" in {
      Given(s"SQL '$alterTable")
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

    "when vacuumed, have old files removed" ignore new SimpleFixture {
      val table: Table = icebergTable(tableName)
      table
        .expireSnapshots()
        .expireOlderThan(System.currentTimeMillis())
        .commit()
      table.expireSnapshots()
      SparkActions
        .get()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("label", newVal))
        .option("target-file-size-bytes", (500 * 1024 * 1024L).toString) // 500 MB
        .execute()
      assert(dataFilesIn(tableName).length < files.size)
    }
  }
}
