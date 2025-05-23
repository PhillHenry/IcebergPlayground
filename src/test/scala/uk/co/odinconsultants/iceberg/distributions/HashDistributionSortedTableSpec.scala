package uk.co.odinconsultants.iceberg.distributions
import uk.co.odinconsultants.iceberg.FileSizeProperties

class HashDistributionSortedTableSpec extends HashDistributionSpec with FileSizeProperties {
  def fileByteSize: Long = 1024*1024*1024

  override protected def otherProperties(partitionField: String): Seq[String] =
    Seq(s"'sort-order' = '$partitionField ASC NULLS FIRST'") ++ fileSizeTableProperties
}
