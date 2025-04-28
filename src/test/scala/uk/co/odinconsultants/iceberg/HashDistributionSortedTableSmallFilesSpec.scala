package uk.co.odinconsultants.iceberg

import uk.co.odinconsultants.SparkForTesting

class HashDistributionSortedTableSmallFilesSpec extends HashDistributionSortedTableOnePartitionSpec {
  override def fileByteSize: Long = 128

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads
}
