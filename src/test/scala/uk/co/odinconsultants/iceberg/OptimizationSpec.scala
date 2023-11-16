package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.{formatSQL, prettyPrintSampleOf}

class OptimizationSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  s"A table" should {
    s"be optimized" in new SimpleFixture {
      override def num_rows: Int = 200000

      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing $num_rows rows to table '$tableName'")
      spark.createDataFrame(data).writeTo(tableName).create()
      val filesBefore = Set(dataFilesIn(tableName))
      val sql =
        s"CALL system.rewrite_data_files(table => \"$tableName\", where => 'partitionKey = ${data.head.partitionKey}')"
      And(s"we execute the SQL ${formatSQL(sql)}")
      spark.sqlContext.sql(sql)
      val filesAfter = Set(dataFilesIn(tableName))
      Then(s"the files added are:\n${(filesBefore -- filesAfter).mkString("\n")}")
      And(s"the files removed are:\n${(filesAfter -- filesBefore).mkString("\n")}")
    }
  }

}
