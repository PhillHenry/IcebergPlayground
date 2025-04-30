package uk.co.odinconsultants.iceberg.distributions

class HashWriteDistributionSortedDataframeSpec extends AbstractDistributionSortedDataframeSpec {

  override def distributionMode(): String = "hash"

}
