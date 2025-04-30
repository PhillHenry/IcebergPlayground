package uk.co.odinconsultants.iceberg.distributions

import uk.co.odinconsultants.documentation_utils.Datum

class HashDistributionSortedTableOnePartitionSpec extends HashDistributionSortedTableSpec {
  override def potentiallyAmendData(xs: Seq[Datum]):Seq[Datum] = xs.map(_.copy(partitionKey = 0))
  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = 1
}
