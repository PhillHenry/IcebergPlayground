package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

import scala.Predef.refArrayOps
import scala.collection.mutable.{Set => MSet}

class CachingSpec extends AnyWordSpec with GivenWhenThen {
  "A dataset to CRUD" should {
    import spark.implicits._
    val tableName           = "spark_file_test_writeTo"

    "create the appropriate Iceberg files" in new SimpleFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      spark.createDataFrame(data).writeTo(tableName).create()
      val output: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(output.collect().size == data.length)

      When("we cache one Dataset and then append more data to the same table")
      output.cache() // In Delta Lake, this would mean any further reads saw the data frozen in time at this point
      spark.createDataFrame(data).writeTo(tableName).append()

      Then("reading via a new Dataset reference shows the appended data")
      val post_cache: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(post_cache.collect().size == data.length * 2)

      And("the old, cached reference still sees the old snapshot of data")
      assert(output.collect().size == data.length)
    }
  }
}
