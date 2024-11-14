package uk.co.odinconsultants.iceberg

import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

class NoneWriteDistributionSortedTableSpec extends AbstractWriteDistributionSpec {
  override def distributionMode(): String = "none"

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = SparkForTesting.numThreads * numPartitions

  override protected def tableDDL(tableName: String, partitionField: String): String =
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
       |    'format-version' = '2',
       |    'sort-order' = '$partitionField ASC NULLS FIRST',
       |    'write.distribution-mode' = '${distributionMode}'
       |) PARTITIONED BY ($partitionField); """.stripMargin
}
