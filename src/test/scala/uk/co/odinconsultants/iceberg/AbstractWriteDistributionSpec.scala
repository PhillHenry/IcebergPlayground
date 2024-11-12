package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.numThreads
import uk.co.odinconsultants.documentation_utils.SpecPretifier
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

abstract class AbstractWriteDistributionSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "Using write.distribution-mode" should {
    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      val createSQL: String = tableDDL(tableName, partitionField)
      Given(s"SQL:${formatSQL(createSQL)}")
      spark.sql(createSQL)
      spark.createDataFrame(data).writeTo(tableName).append()
      val before: Seq[String] = parquetFiles(tableName)
      And(s"it has ${data.length} rows over ${before.length} data files when using $numThreads threads")
      assert(before.length == expectedNumberOfFilesPerAppend(num_partitions))
      When(s"we add another ${data.length} rows of the same data that is logically distributed over $num_partitions partitions")
      spark.createDataFrame(data).writeTo(tableName).append()
      val after: Seq[String] = parquetFiles(tableName)
      val diff = after.length - before.length
      Then(s"there are now ${diff} more data files")
      assert(diff > 0)
      assert(after.length == expectedNumberOfFilesPerAppend(num_partitions) * 2)
    }
  }

  protected def distributionMode: String

  protected def expectedNumberOfFilesPerAppend(numPartitions: Int): Int

  private def tableDDL(tableName: String, partitionField: String): String = {
    val createSQL: String = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.distribution-mode' = '${distributionMode}'
                               |) PARTITIONED BY ($partitionField); """.stripMargin
    createSQL
  }
}
