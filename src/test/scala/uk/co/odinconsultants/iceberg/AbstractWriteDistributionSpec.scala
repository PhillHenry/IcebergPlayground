package uk.co.odinconsultants.iceberg
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.numThreads
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

abstract class AbstractWriteDistributionSpec
    extends SpecPretifier
    with GivenWhenThen
    with TableNameFixture {

  info("https://iceberg.apache.org/docs/1.6.0/spark-writes/#writing-distribution-modes")

  "Using write.distribution-mode" should {
    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      val createSQL: String   = tableDDL(tableName, partitionField)
      Given(s"a table that is created with:${formatSQL(createSQL)}")
      spark.sql(createSQL)
      appendData(spark, data)
      val before: Seq[String] = parquetFiles(tableName)
      And(
        s"it has ${data.length} rows over ${before.length} data files when using $numThreads threads"
      )
      assert(before.length == expectedNumberOfFilesPerAppend(num_partitions))
      When(
        s"we add another ${data.length} rows of the same data that is logically distributed over $num_partitions partitions"
      )
      appendData(spark, data)
      val after: Seq[String]  = parquetFiles(tableName)
      val diff                = after.length - before.length
      Then(s"there are now ${diff} more data files")
      assert(diff > 0)
      assert(after.length == expectedNumberOfFilesPerAppend(num_partitions) * 2)
    }
  }

  protected def appendData(
      spark: SparkSession,
      data: Seq[Datum],
  ): Unit =
    spark.createDataFrame(data).writeTo(tableName).append()

  protected def distributionMode: String

  protected def expectedNumberOfFilesPerAppend(numPartitions: Int): Int

  private def tableDDL(tableName: String, partitionField: String): String =
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.distribution-mode' = '${distributionMode}'
                               |) PARTITIONED BY ($partitionField); """.stripMargin
}
