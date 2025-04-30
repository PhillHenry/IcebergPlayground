package uk.co.odinconsultants.iceberg.distributions

import uk.co.odinconsultants.SparkForTesting

class NoneWriteDistributionSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads * numPartitions
}
