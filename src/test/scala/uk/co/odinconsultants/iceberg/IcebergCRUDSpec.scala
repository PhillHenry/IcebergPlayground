package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf
import org.scalatest._


class IcebergCRUDSpec extends AnyWordSpec with GivenWhenThen {
  import org.apache.spark.sql.Encoders._
  "A dataset to CRUD" should {
    import spark.implicits._
    val tableName                = "spark_file_test_writeTo"

    "create the appropriate Iceberg files" in new SimpleFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      Then("reading the table back yields the same data")
      val output: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(output.collect().toSet == data.toSet)
    }

    val newVal    = "ipse locum"
    val updateSql = s"update $tableName set label='$newVal'"
    s"support updates with '$updateSql'" in new SimpleFixture {
      Given(s"SQL '$updateSql''")
      When("we execute it")
      spark.sqlContext.sql(updateSql)
      Then("all rows are updated")
      val output: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      val rows: Array[Datum]  = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      assert(rows.length == data.length)
      for {
        row <- rows
      } yield assert(row.label == newVal)
    }
  }
}
