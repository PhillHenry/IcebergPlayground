package uk.co.odinconsultants.iceberg.distributions

import uk.co.odinconsultants.SparkForTesting

class NoneWriteDistributionSortedTableSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads * numPartitions

  override protected def otherProperties(partitionField: String): Seq[String] = Seq(
    s"'sort-order' = '$partitionField ASC NULLS FIRST'"
  )
}
