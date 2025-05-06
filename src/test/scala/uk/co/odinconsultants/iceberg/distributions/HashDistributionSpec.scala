package uk.co.odinconsultants.iceberg.distributions

class HashDistributionSpec extends AbstractWriteDistributionSpec {

  override def distributionMode(): String = "hash"

  override def expectedNumberOfFilesPerAppend(numLogicalPartitions: Int): Int = numLogicalPartitions
}
