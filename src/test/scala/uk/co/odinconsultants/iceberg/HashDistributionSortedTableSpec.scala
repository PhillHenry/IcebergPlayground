package uk.co.odinconsultants.iceberg

class HashDistributionSortedTableSpec extends HashDistributionSpec {
  override protected def otherProperties(partitionField: String): Seq[String] =
    Seq(s"'sort-order' = '$partitionField ASC NULLS FIRST'",
      s"'spark.sql.adaptive.advisoryPartitionSizeInBytes' = '${1024*1024*1024}'"
    )
}
