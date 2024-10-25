package uk.co.odinconsultants.iceberg
import uk.co.odinconsultants.SparkForTesting.{namespace, catalog}

trait TableNameFixture {
  val tableName = catalog + "." + namespace + "." + this.getClass.getSimpleName.replace("$", "_")
}
