package uk.co.odinconsultants.iceberg

class MergeOnReadSpec extends AbstractCrudSpec {

  override def tableName = "mor_table"
  override def mode = "merge-on-read"

  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
  ): Unit = {
    val newFiles: Set[String] = current -- previous
    newFiles.foreach { file: String =>
      assert(file.endsWith("00001.parquet") || file.endsWith("00001.parquet.crc") || file.endsWith("00001-deletes.parquet.crc") || file.endsWith("00001-deletes.parquet"))
    }
  }
}
