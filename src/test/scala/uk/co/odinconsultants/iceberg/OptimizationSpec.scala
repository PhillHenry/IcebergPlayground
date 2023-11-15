package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

class OptimizationSpec extends AnyWordSpec with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  s"A table" should {
    s"be optimized" in new SimpleFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      spark.sqlContext.sql(
      s"CALL system.rewrite_data_files(table => \"$tableName\", where => 'id = ${data.head.id} and label = \"${data.head.label}\"')"
      )
    }
  }

}
