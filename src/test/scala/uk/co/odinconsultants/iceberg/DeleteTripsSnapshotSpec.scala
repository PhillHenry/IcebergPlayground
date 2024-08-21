package uk.co.odinconsultants.iceberg

import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.SparkForTesting.{catalog, namespace}
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}


class DeleteTripsSnapshotSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/1.5.1/spark-procedures/#snapshot")

  "A snapshot" should {
    "blow up if the underlying data is deleted" ignore new SimpleSparkFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      val dst_table = s"${namespace}.${tableName}_dst"
      val src_table = s"${namespace}.$tableName"
      And(s"$num_rows rows are initially written to table '$src_table'")
      spark.createDataFrame(data).writeTo(src_table).create()
      val sql =
        s"""CALL system.snapshot('$src_table',
           |'$dst_table')""".stripMargin
      When(s"we execute the SQL:${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
    }
    s"be cleaned up with remove_orphan_files" ignore new SimpleSparkFixture {
      val filesBefore = dataFilesIn(tableName).toSet
      Given(s"there are already ${filesBefore.size} files for table $tableName")
      val fqn: String = s"${SparkForTesting.namespace}." + tableName
      val sql =
        s"""CALL system.remove_orphan_files(table => \"$tableName\")""".stripMargin
      When(s"we execute the SQL:${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
      val filesAfter = dataFilesIn(tableName).toSet
      Then(s"old files have been removed and only ${filesAfter.size} remain")
      assert(filesAfter.size < filesBefore.size)
    }
  }
}
