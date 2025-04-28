package uk.co.odinconsultants.iceberg

trait FileSizeProperties {
  def fileByteSize: Long

  def fileSizeTableProperties: Seq[String] =
    Seq(
      s"'spark.sql.adaptive.advisoryPartitionSizeInBytes' = '$fileByteSize'",
      s"'write.spark.advisory-partition-size-bytes' = '$fileByteSize'",
      s"'write.target-file-size-bytes' = '$fileByteSize'"
    )
}
