package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.documentation_utils.Datum

abstract class AbstractDistributionSortedDataframeSpec extends AbstractWriteDistributionSpec {

  override protected def appendData(
                                     spark: SparkSession,
                                     data: Seq[Datum],
                                   ): Unit = {

    val sortField: String = TestUtils.partitionField
    And(s"the data is sorted on the $sortField column")
    spark.createDataFrame(data).sort(sortField).writeTo(tableName).append()
  }

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = numPartitions
}
