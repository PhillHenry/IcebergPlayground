package uk.co.odinconsultants.iceberg
import uk.co.odinconsultants.SparkForTesting.namespace

trait TableNameFixture {
  val tableName = "polaris." + namespace + "." + this.getClass.getSimpleName.replace("$", "_")
}
