package uk.co.odinconsultants.iceberg.distributions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.numThreads
import uk.co.odinconsultants.TextUtils.{emphasise, highlight}
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.createDatumTable
import uk.co.odinconsultants.iceberg.{SimpleSparkFixture, TableNameFixture, TestUtils}

abstract class AbstractWriteDistributionSpec
    extends SpecPretifier
    with GivenWhenThen
    with TableNameFixture {

  info("See https://iceberg.apache.org/docs/1.6.0/spark-writes/#writing-distribution-modes")

  "Using write.distribution-mode" should {
    "create the appropriate number of Iceberg files" in new SimpleSparkFixture {
      val createSQL: String   = tableDDL(tableName, partitionField)
      private val sql: String = otherProperties(partitionField).foldLeft(formatSQL(createSQL))((acc, x) => emphasise(x, acc, Console.YELLOW))
      Given(s"a table that has a distribution mode of ${highlight(distributionMode)}\nand is created with:$sql")
      spark.sql(createSQL)
      private val dataToWrite: Seq[Datum] = potentiallyAmendData(data)
      val df = appendData(spark, dataToWrite)
      And("a query plan that looks like:\n" + captureOutputOf(
        df.explain(true)
      ))
      val before: Seq[String] = parquetFiles(tableName)
      And(
        s"it has ${data.length} rows over ${before.length} data file(s) when writing with $numThreads executor threads"
      )
      assert(before.length == expectedNumberOfFilesPerAppend(num_partitions))
      When(
        s"we add another ${data.length} rows of the same data that is logically distributed over ${dataToWrite.map(_.partitionKey).toSet.size} partition(s)"
      )
      appendData(spark, dataToWrite)
      val after: Seq[String]  = parquetFiles(tableName)
      val diff                = after.length - before.length
      Then(s"there are now ${diff} more data files")
      assert(diff > 0)
      assert(after.length == expectedNumberOfFilesPerAppend(num_partitions) * 2)
    }
  }

  protected def potentiallyAmendData(xs: Seq[Datum]):Seq[Datum] = xs

  protected def appendData(
      spark: SparkSession,
      data: Seq[Datum],
  ): DataFrame = {
    val df: DataFrame = dataFrame(spark, data)
    df.writeTo(tableName).append()
    df
  }

  def dataFrame(spark: SparkSession, data: Seq[Datum]) = {
    val x = spark.createDataFrame(data)
    x
//    val y = spark.range(1000).withColumnRenamed("id", "otherId")
//    val df = x.join(y, x(TestUtils.partitionField) === y("otherId"), "outer")
//    df.drop("otherId")
  }

  protected def distributionMode: String

  protected def expectedNumberOfFilesPerAppend(numPartitions: Int): Int

  protected def tableDDL(tableName: String, partitionField: String): String = {
    val tblProperties = Seq(s"'write.distribution-mode' = '${distributionMode}'") ++ otherProperties(partitionField)
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    ${tblProperties.mkString(",\n    ")}
                               |) PARTITIONED BY ($partitionField); """.stripMargin
  }

  protected def otherProperties(partitionField: String): Seq[String] = Seq.empty[String]
}
