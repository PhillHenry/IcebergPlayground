package uk.co.odinconsultants.iceberg
import uk.co.odinconsultants.SparkForTesting.spark

class CopyOnWriteSpec extends AbstractCrudSpec {

  import spark.implicits._

  override def tableName = "cow_table"
  override def mode      = "copy-on-write"
  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
      changes: Set[Datum],
  ): Unit = {
    val newFiles: Set[String]     = current -- previous
    assert(newFiles.nonEmpty)
    newFiles.foreach { file: String =>
      assert(file.endsWith("00001.parquet.crc") || file.endsWith("00001.parquet"))
    }
    val parquetFiles: Set[String] = newFiles.filter(_.endsWith(".parquet"))
    assert(parquetFiles.nonEmpty)
    parquetFiles.foreach { file: String =>
      val actual: Array[Datum] = spark.read.parquet(file).as[Datum].collect()
      assert(changes.size == actual.length)
      assert(changes == actual.toSet)
      assert(actual.length == changes.size)
      And(s"the new data file contains just the updated row(s)")
    }
  }
}
