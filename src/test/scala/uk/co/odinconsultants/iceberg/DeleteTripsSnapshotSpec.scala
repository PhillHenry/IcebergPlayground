package uk.co.odinconsultants.iceberg

import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.SparkForTesting.{catalog, namespace}
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}


class DeleteTripsSnapshotSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/1.5.1/spark-procedures/#snapshot")

  "A snapshot" should {
    "blow up if the underlying data is deleted" ignore new SimpleSparkFixture {
      val spark_ns = "spark_catalog"
      val dst_table = s"$spark_ns.${tableName}_dst".toLowerCase()
      val src_table = s"$spark_ns.$tableName".toLowerCase()
      Given(s"data in $src_table that looks like:\n${prettyPrintSampleOf(data)}")
      // spark_catalog.$tableName => [REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, but got .
//      spark.sql(s"create database $spark_ns")
      And(s"$num_rows rows are initially written to table '$src_table'")
      spark.createDataFrame(data).writeTo(src_table).create() // this needs the DB to be created

      private val partitionSQL = s"ALTER TABLE $src_table ADD PARTITION FIELD partition"
      And(s"we add a partition spec with:\n${formatSQL(partitionSQL)}")
      spark.sql(partitionSQL)

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
