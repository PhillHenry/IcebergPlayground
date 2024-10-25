package uk.co.odinconsultants.iceberg
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import java.text.SimpleDateFormat

/**
 * Amogh Jahagirdar
Expires snapshots does do a file cleanup as well, it's specifically cleaning up files which are no longer referenced after the snapshots are removed.
Remove orphan files is requires a file listing to determine which files are currently in the table location and not in the table state. Since FileIO's don't have a notion of listing, the Spark procedure uses the hadoop fs configuration for setting up a HDFS style file system and perform the listing. Although my information on the FileIO listing maybe dated, there was a SupportsPrefixListing mixin interface added, but I'd need to double check the integration into the procedure. But based on what you're seeing this should still apply.
 Expires snapshots does not perform a listing.

 */
class OptimizationSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  info("https://iceberg.apache.org/docs/1.4.2/spark-procedures/")

  val MIN_INPUT_FILES: Int = 2

  s"A table that has many files" should {
    s"have those files aggregated" in new SimpleSparkFixture {
      override def num_rows: Int = 20000

      Given(s"data\n${prettyPrintSampleOf(data)}")
      And(s"$num_rows rows are initially written to table '$tableName'")
      spark.sql(s"DROP TABLE  IF EXISTS $tableName  PURGE")
      spark.createDataFrame(data).writeTo(tableName).create()

      val filesBefore = parquetFiles(tableName).toSet
      val sql =
        s"""CALL system.rewrite_data_files(table => \"$tableName\",
           |options => map('min-input-files','$MIN_INPUT_FILES'))""".stripMargin
      When(s"we execute the SQL ${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
      val filesAfter = parquetFiles(tableName).toSet
      val added: Set[String] = filesAfter -- filesBefore
      Then(s"the files added to the original ${filesBefore.size} are:\n${toHumanReadable(added)}")
      val deleted: Set[String] = filesBefore -- filesAfter
      And(s"there are no files deleted from the subsequent ${filesAfter.size}")
      assert(deleted.size == 0)
      And("these new files contain all the data")
      val addedDf: Dataset[Datum] = spark.read.parquet(added.toArray.map(_.toString): _*).as[Datum]
      private val fromNewFiles: Array[Datum] = addedDf.collect()
      assert(fromNewFiles.length == num_rows)
      assert(diffHavingOrdered(fromNewFiles).isEmpty)
    }
    s"have snapshots removed when expired" in new SimpleSparkFixture {
      val filesBefore = parquetFiles(tableName).toSet
      Given(s"there are already ${filesBefore.size} files for table $tableName")
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val sql =
        s"""CALL system.expire_snapshots(table => \"$tableName\",
           |older_than => TIMESTAMP '${dateFormat.format(new java.util.Date())}',
           |stream_results => true)""".stripMargin
      When(s"we execute the SQL:${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
      val filesAfter = parquetFiles(tableName).toSet
      Then(s"old files have been removed and only ${filesAfter.size} remain")
      assert(filesAfter.size < filesBefore.size)
    }
  }

}
