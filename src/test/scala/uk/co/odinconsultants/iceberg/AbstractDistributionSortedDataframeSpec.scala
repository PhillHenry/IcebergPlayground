package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.odinconsultants.documentation_utils.Datum

abstract class AbstractDistributionSortedDataframeSpec extends AbstractWriteDistributionSpec {

  override protected def appendData(
                                     spark: SparkSession,
                                     data: Seq[Datum],
                                   ): DataFrame = {

    val sortField: String = TestUtils.partitionField
    And(s"the data is sorted on the $sortField column")
    val df = spark.createDataFrame(data).sort(sortField)
    df.writeTo(tableName).append()
    df
  }

  override def expectedNumberOfFilesPerAppend(numPartitions: Int): Int = numPartitions
}
