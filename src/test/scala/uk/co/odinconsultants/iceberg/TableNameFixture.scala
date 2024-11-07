package uk.co.odinconsultants.iceberg
import uk.co.odinconsultants.SparkForTesting.{namespace, catalog}
import uk.co.odinconsultants.SparkForTesting.spark

trait TableNameFixture {
  val tableName = catalog + "." + namespace + "." + this.getClass.getSimpleName.replace("$", "_")

  private val purgeSql = s"DROP TABLE  IF EXISTS $tableName  PURGE"
  println(s"About to run:\n$purgeSql")
  spark.sql(purgeSql)
}