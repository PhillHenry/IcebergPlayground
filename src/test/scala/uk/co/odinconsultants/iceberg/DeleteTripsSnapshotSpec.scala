package uk.co.odinconsultants.iceberg

import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.TextUtils.{Emphasis, emphasise, highlight}
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.{createDatumTable, insertSQL}


class DeleteTripsSnapshotSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/1.5.1/spark-procedures/#snapshot")

  "A snapshot" should {
    val db = "my_db"
    val catalog = "local"
    val spark_ns = s"$catalog.$db"
    val dst_table = s"$spark_ns.${tableName}_dst".toLowerCase()
    val src_table = s"$spark_ns.$tableName".toLowerCase()
    "blow up if the underlying data is deleted" ignore new SimpleSparkFixture {
      spark.sql(s"USE $catalog")
      Given(s"data in $src_table that looks like:\n${prettyPrintSampleOf(data)}")
      // spark_catalog.$tableName => [REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, but got .
      spark.sql(s"create database $spark_ns")
      And(s"$num_rows rows are initially written to table '$src_table'")
      spark.createDataFrame(data).writeTo(src_table).using("iceberg").create() // this needs the DB to be created
      spark.catalog.listTables(spark_ns).show()
      spark.table(src_table).show()
//      spark.read.table(src_table).show()

      private val partitionSQL = s"ALTER TABLE $src_table ADD PARTITION FIELD partitionKey"
      And(s"we add a partition spec with:\n${formatSQL(partitionSQL)}")
      spark.sql(partitionSQL)

      val sql =
        s"""CALL system.snapshot(
           |'$src_table',
           |'$dst_table')""".stripMargin
      When(s"we execute the SQL:${highlight(emphasise("snapshot", sql, Emphasis))}")
      spark.sqlContext.sql(sql)
    }
    val mode = "copy-on-write"
    val createSQL: String                                = tableDDL(src_table, mode)
    s"create no new files for $mode" ignore new SimpleSparkFixture {
      spark.sql(s"create database $spark_ns")
      spark.sql(s"DROP TABLE IF EXISTS $src_table")
      spark.sql(s"DROP TABLE IF EXISTS $db.$tableName")
      Given(s"SQL:${formatSQL(createSQL)}")
      When("we execute it")
      spark.sqlContext.sql(createSQL)
    }
    s"insert creates new files for $mode" ignore new SimpleSparkFixture {
      spark.sql(s"USE $catalog")
      val sql: String           = insertSQL(src_table, data)
      Given(s"SQL:${formatSQL(sql)}")
      When("we execute it")
      spark.sqlContext.sql(sql)

      val snapshotSQL =
        s"""CALL $catalog.system.snapshot(
           |'$src_table',
           |'$dst_table')""".stripMargin
      When(s"we execute the SQL:${highlight(emphasise("snapshot", snapshotSQL, Emphasis))}")
      spark.sqlContext.sql(snapshotSQL)
    }
    s"be cleaned up with remove_orphan_files" ignore new SimpleSparkFixture {
      val filesBefore = parquetFiles(tableName).toSet
      Given(s"there are already ${filesBefore.size} files for table $tableName")
      val fqn: String = s"${SparkForTesting.namespace}." + tableName
      val sql =
        s"""CALL system.remove_orphan_files(
           |table => \"$tableName\"
           )""".stripMargin
      When(s"we execute the SQL:${highlight(emphasise("remove_orphan_files", sql, Emphasis))}")
      spark.sqlContext.sql(sql)
      val filesAfter = parquetFiles(tableName).toSet
      Then(s"old files have been removed and only ${filesAfter.size} remain")
      assert(filesAfter.size < filesBefore.size)
    }
  }
  private def tableDDL(tableName: String, mode: String): String = {
    val createSQL: String = s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '1',
                               |    'write.delete.mode'='$mode',
                               |    'write.update.mode'='$mode',
                               |    'write.merge.mode'='$mode'
                               |) PARTITIONED BY (${classOf[
      Datum
    ].getDeclaredFields.filter(_.getName.toLowerCase.contains("partition")).head.getName}); """.stripMargin
    createSQL
  }
}
