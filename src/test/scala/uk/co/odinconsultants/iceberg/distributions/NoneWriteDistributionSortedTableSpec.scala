package uk.co.odinconsultants.iceberg.distributions

class NoneWriteDistributionSortedTableSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numLogicalPartitions: Int): Int = NUM_DF_PARTITIONS * numLogicalPartitions

  override protected def otherProperties(partitionField: String): Seq[String] = Seq(
    s"'sort-order' = '$partitionField ASC NULLS FIRST'"
  )
}
