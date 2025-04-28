package uk.co.odinconsultants.iceberg

import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.documentation_utils.Datum

class NoneWriteDistributionSortedTableOnePartitionSpec extends NoneWriteDistributionSortedTableSpec with FileSizeProperties {
  info("The corner case of all the data being in one partition")

  def fileByteSize: Long = 1024*1024*1024
  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads
  override def potentiallyAmendData(xs: Seq[Datum]):Seq[Datum] = xs.map(_.copy(partitionKey = 0))
  override protected def otherProperties(partitionField: String): Seq[String] =
    Seq(s"'sort-order' = '$partitionField ASC NULLS FIRST'") ++  fileSizeTableProperties
}
