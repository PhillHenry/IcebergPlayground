package uk.co.odinconsultants.iceberg.distributions

class HashDistributionSpec extends AbstractWriteDistributionSpec {

  override def distributionMode(): String = "hash"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = numPartitions
}
