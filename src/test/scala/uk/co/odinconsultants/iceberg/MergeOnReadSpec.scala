package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.SparkForTesting._

class MergeOnReadSpec extends AbstractCrudSpec {

  override def tableName = "mor_table"
  override def mode = "merge-on-read"

  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
  ): Unit = {
    val newFiles: Set[String] = current -- previous
    newFiles.foreach { file: String =>
      assert(file.endsWith("00001.parquet")
        || file.endsWith("00001.parquet.crc")
        || file.endsWith("00001-deletes.parquet.crc")
        || file.endsWith("00001-deletes.parquet"))
    }
    newFiles.filter(_.endsWith("00001-deletes.parquet")).foreach { file: String =>
      val df: DataFrame = spark.read.parquet(file)
      df.show(false)
      assert(df.count() == 1)
    }
  }
}
