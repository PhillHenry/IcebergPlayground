package uk.co.odinconsultants.iceberg

class CopyOnWriteSpec extends AbstractCrudSpec {

  override def tableName = "cow_table"
  override def mode      = "copy-on-write"
  override def checkDatafiles(
      previous: Set[String],
      current: Set[String],
  ): Unit = {
    val newFiles: Set[String] = current -- previous
    newFiles.foreach { file: String =>
      assert(file.endsWith("00001.parquet.crc") || file.endsWith("00001.parquet"))
    }
  }
}
