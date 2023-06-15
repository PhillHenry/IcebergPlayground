package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

class IcebergCRUDSpec extends AnyWordSpec with GivenWhenThen {
  val IntField: String = "_1"
  "A dataset to CRUD" should {
    val data: Seq[(Int, String)] = Seq((41, "phill"), (42, "henry"))
    val df: DataFrame            = spark.createDataFrame(data)
    val tableName                = "spark_file_test_writeTo"

    "create the appropriate Iceberg files" in {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      df.writeTo(tableName).create()
      Then("reading the table back yields the same data")
      val output: DataFrame = spark.read.table(tableName)
      assert(output.collect().length == data.length)
    }

    val newVal    = "ipse locum"
    val updateSql = s"update $tableName set _2='$newVal'"
    s"support updates with '$updateSql'" in {
      Given(s"SQL '$updateSql''")
      When("we execute it")
      spark.sqlContext.sql(updateSql)
      Then("all rows are updated")
      val output: DataFrame = spark.read.table(tableName)
      val rows: Array[Row]  = output.collect()
      assert(rows.length == data.length)
      for {
        row <- rows
      } yield assert(row.getString(1) == newVal)
    }
  }
}
