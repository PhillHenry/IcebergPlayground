package uk.co.odinconsultants.iceberg
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

class CachingSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "A dataset to CRUD" should {
    info("Unlike DeltaLake, Iceberg does not freeze data in time after a call to .cache()")
    import spark.implicits._

    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      Given(s"data files\n${prettyPrintSampleOf(data)}")
      spark.sql(s"DROP TABLE  IF EXISTS $tableName  PURGE")
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
