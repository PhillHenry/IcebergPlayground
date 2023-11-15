package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

class OptimizationSpec extends AnyWordSpec with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  s"A table" should {
    s"be optimized" ignore new SimpleFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      "CALL catalog_name.system.rewrite_data_files(table => 'db.sample', where => 'id = 3 and name = \"foo\"')"
    }
  }

}
