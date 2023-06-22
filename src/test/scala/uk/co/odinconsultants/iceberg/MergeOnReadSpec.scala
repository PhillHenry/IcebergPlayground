package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.SparkForTesting._

class MergeOnReadSpec extends AbstractCrudSpec {

  import spark.implicits._

  override def tableName = "mor_table"
  override def mode = "merge-on-read"

  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
      changes: Set[Datum]
  ): Unit = {
    val newFiles: Set[String] = current -- previous
    newFiles.foreach { file: String =>
      assert(file.endsWith("00001.parquet")
        || file.endsWith("00001.parquet.crc")
        || file.endsWith("00001-deletes.parquet.crc")
        || file.endsWith("00001-deletes.parquet"))
    }
    newFiles.filter(_.endsWith(".parquet")).foreach { file: String =>
      if (file.contains("-deletes.")) {
        val df: DataFrame = spark.read.parquet(file)
        assert(df.count() == 1)  // one row changed, one file changed
      } else {
        val actual: Array[Datum] = spark.read.parquet(file).as[Datum].collect()
        And(s"the new parquet file contains:\n${toHumanReadable(actual)}")
        assert(actual.toSet == changes)
      }
    }
  }
}
