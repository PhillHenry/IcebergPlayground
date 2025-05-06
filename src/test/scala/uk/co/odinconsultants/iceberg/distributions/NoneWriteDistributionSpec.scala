package uk.co.odinconsultants.iceberg.distributions

class NoneWriteDistributionSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = NUM_DF_PARTITIONS * numPartitions
}
