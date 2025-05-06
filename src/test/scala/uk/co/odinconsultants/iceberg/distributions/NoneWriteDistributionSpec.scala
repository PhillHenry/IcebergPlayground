package uk.co.odinconsultants.iceberg.distributions

class NoneWriteDistributionSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numLogicalPartitions: Int): Int = NUM_DF_PARTITIONS * numLogicalPartitions
}
