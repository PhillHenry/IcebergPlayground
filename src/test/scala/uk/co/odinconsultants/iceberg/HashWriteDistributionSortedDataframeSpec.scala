package uk.co.odinconsultants.iceberg

class HashWriteDistributionSortedDataframeSpec extends AbstractDistributionSortedDataframeSpec {

  override def distributionMode(): String = "hash"

}
