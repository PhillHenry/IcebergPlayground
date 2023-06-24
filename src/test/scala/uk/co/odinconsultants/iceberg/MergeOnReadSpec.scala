package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.{DataFrame, Row}
import uk.co.odinconsultants.SparkForTesting._

class MergeOnReadSpec extends AbstractCrudSpec {

  import spark.implicits._

  override def tableName = "mor_table"
  override def mode      = "merge-on-read"

  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
      changes: Set[Datum],
  ): Unit = {
    val newFiles: Set[String]       = current -- previous
    newFiles.foreach { file: String =>
      assert(
        file.endsWith("00001.parquet")
          || file.endsWith("00001.parquet.crc")
          || file.endsWith("00001-deletes.parquet.crc")
          || file.endsWith("00001-deletes.parquet")
      )
    }
    val parquetFiles: Set[String]   = newFiles.filter(_.endsWith(".parquet"))
    val deleteFiles: Set[String]    = parquetFiles.filter(_.contains("-deletes."))
    assert(deleteFiles.nonEmpty)
    deleteFiles.foreach { file: String =>
      val df: DataFrame       = spark.read.parquet(file)
      df.show(false)
      val deletes: Array[Row] = df.collect()
      assert(deletes.length == changes.size)
      deletes.foreach { row: Row =>
        val file_path: String              = row.getString(0)
        val pos: Long                      = row.getLong(1)
        val persistedChanges: Array[Datum] = spark.read.parquet(file_path).as[Datum].collect()
        assert(changes.map(_.id).contains(persistedChanges(pos.toInt).id))
      }
    }
    val nonDeleteFiles: Set[String] = parquetFiles.filter(!_.contains("-deletes."))
    assert(nonDeleteFiles.nonEmpty)
    nonDeleteFiles.foreach { file: String =>
      val actual: Array[Datum] = spark.read.parquet(file).as[Datum].collect()
      And(s"the new parquet file contains:\n${toHumanReadable(actual)}")
      assert(actual.toSet == changes)
    }
  }
}
