package uk.co.odinconsultants.iceberg


import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.numThreads
import uk.co.odinconsultants.documentation_utils.SpecPretifier
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

import scala.collection.mutable.{Set => MSet}


class WriteDistributionSpec  extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "Using write.distribution-mode" should {
    val files: MSet[String] = MSet.empty[String]

    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      val createSQL: String = tableDDL(tableName, partitionField)
      Given(s"SQL:${formatSQL(createSQL)}")
      spark.sql(createSQL)
      spark.createDataFrame(data).writeTo(tableName).append()
      val before: Seq[String] = parquetFiles(tableName)
      And(s"it has ${data.length} rows over ${before.length} data files when using $numThreads threads")
      assert(before.length == num_partitions)
      When(s"we add ${data.length} more rows")
      spark.createDataFrame(data).writeTo(tableName).append()
      val after: Seq[String] = parquetFiles(tableName)
      val diff = after.length - before.length
      Then(s"there are now ${diff} more data files")
      assert(diff > 0)
      assert(after.length == num_partitions * 2)
    }
  }


  private def tableDDL(tableName: String, partitionField: String): String = {
    val createSQL: String = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.distribution-mode' = 'hash'
                               |) PARTITIONED BY ($partitionField); """.stripMargin
    createSQL
  }
}
