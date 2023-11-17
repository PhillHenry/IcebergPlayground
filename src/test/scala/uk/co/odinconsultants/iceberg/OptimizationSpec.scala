package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.{formatSQL, prettyPrintSampleOf, toHumanReadable}
import uk.co.odinconsultants.iceberg.SQL.insertSQL

class OptimizationSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  s"A table" should {
    s"be optimized" in new SimpleFixture {
      override def num_rows: Int = 20000

      Given(s"data\n${prettyPrintSampleOf(data)}")
      And(s"$num_rows rows are initially written to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()

      val filesBefore = dataFilesIn(tableName).toSet
      val sql =
        s"CALL system.rewrite_data_files(table => \"$tableName\", options => map('min-input-files','2'))"
      When(s"we execute the SQL ${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
      val filesAfter = dataFilesIn(tableName).toSet
      val added: Set[String] = filesAfter -- filesBefore
      Then(s"the files added to the original ${filesBefore.size} are:\n${toHumanReadable(added)}")
      val deleted: Set[String] = filesBefore -- filesAfter
      And(s"there are no files deleted from the subsequent ${filesAfter.size}")
      assert(deleted.size == 0)
    }
  }

}
