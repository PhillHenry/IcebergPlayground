package uk.co.odinconsultants.iceberg
import org.apache.commons.io.FileUtils
import uk.co.odinconsultants.SparkForTesting.{catalog, namespace}
import uk.co.odinconsultants.SparkForTesting.spark

import java.io.File

trait TableNameFixture {
  private val simpleTableName: String = this.getClass.getSimpleName.replace("$", "_")
  val tableName = catalog + "." + namespace + "." + simpleTableName

  private val purgeSql = s"DROP TABLE  IF EXISTS $tableName  PURGE"
  println(s"About to run:\n$purgeSql")
  spark.sql(purgeSql)

  private val dir = s"/tmp/$catalog/$namespace/$simpleTableName"
  println(s"About to delete $dir")
  FileUtils.deleteDirectory(new File(dir))
}
