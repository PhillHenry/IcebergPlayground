package uk.co.odinconsultants.iceberg

class HashDistributionSortedTableSpec extends HashDistributionSpec {
  def fileByteSize: Long = 1024 * 1024 * 1024
  override protected def otherProperties(partitionField: String): Seq[String] =
    Seq(s"'sort-order' = '$partitionField ASC NULLS FIRST'",
      s"'spark.sql.adaptive.advisoryPartitionSizeInBytes' = '$fileByteSize'",
      s"'write.spark.advisory-partition-size-bytes' = '$fileByteSize'",
      s"'write.target-file-size-bytes' = '$fileByteSize'"
    )
}
