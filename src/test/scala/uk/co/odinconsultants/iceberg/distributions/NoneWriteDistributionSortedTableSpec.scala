package uk.co.odinconsultants.iceberg.distributions

class NoneWriteDistributionSortedTableSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = NUM_DF_PARTITIONS * numPartitions

  override protected def otherProperties(partitionField: String): Seq[String] = Seq(
    s"'sort-order' = '$partitionField ASC NULLS FIRST'"
  )
}
