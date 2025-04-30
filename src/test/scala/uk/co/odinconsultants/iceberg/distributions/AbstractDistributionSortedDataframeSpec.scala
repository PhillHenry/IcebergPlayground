package uk.co.odinconsultants.iceberg.distributions

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.odinconsultants.documentation_utils.Datum
import uk.co.odinconsultants.iceberg.TestUtils

abstract class AbstractDistributionSortedDataframeSpec extends AbstractWriteDistributionSpec {

  override protected def appendData(
                                     spark: SparkSession,
                                     data: Seq[Datum],
                                   ): DataFrame = {

    val sortField: String = TestUtils.partitionField
    And(s"the data is sorted on the $sortField column")
    val df = dataFrame(spark, data).sort(sortField)
    df.writeTo(tableName).append()
    df
  }

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = numPartitions
}
