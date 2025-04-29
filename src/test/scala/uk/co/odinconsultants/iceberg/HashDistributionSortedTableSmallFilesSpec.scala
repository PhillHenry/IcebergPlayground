package uk.co.odinconsultants.iceberg

import uk.co.odinconsultants.SparkForTesting

class HashDistributionSortedTableSmallFilesSpec extends HashDistributionSortedTableOnePartitionSpec {
  override def fileByteSize: Long = 128

  /**
   * For Spark 3.3, this fails (passes for 3.5).
   * The number of files per append in Spark 3.3 is 1, even when the files are small.
   */
  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads
}
