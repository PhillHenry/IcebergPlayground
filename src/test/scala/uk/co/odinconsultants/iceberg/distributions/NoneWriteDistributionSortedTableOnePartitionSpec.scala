package uk.co.odinconsultants.iceberg.distributions

import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.documentation_utils.Datum
import uk.co.odinconsultants.iceberg.FileSizeProperties

class NoneWriteDistributionSortedTableOnePartitionSpec extends NoneWriteDistributionSortedTableSpec with FileSizeProperties {
  info("The corner case of all the data being in one partition")

  def fileByteSize: Long = 1024*1024*1024
  override def expectedNumberOfFilesPerAppend(numLogicalPartitions: Int): Int = NUM_DF_PARTITIONS
  override def potentiallyAmendData(xs: Seq[Datum]):Seq[Datum] = xs.map(_.copy(partitionKey = 0))
  override protected def otherProperties(partitionField: String): Seq[String] =
    Seq(s"'sort-order' = '$partitionField ASC NULLS FIRST'") ++  fileSizeTableProperties
}
