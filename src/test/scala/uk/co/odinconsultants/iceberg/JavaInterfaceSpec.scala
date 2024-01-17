package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopFileIO
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class JavaInterfaceSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "Iceberg tables" should {
    "have its history queried" in new SimpleSparkFixture {
      Given("a table that has seen changes")
      spark.createDataFrame(data).writeTo(tableName).create()
      val updateSql = s"update $tableName set label='newVal'"
      spark.sqlContext.sql(updateSql)

      When(s"we query the table")

      val table: Table = icebergTable(tableName)
      val filenames    = MetaUtils.allFilesThatmake(table, new HadoopFileIO(hadoopConfig))

      Then(s"the filenames are:\n${filenames.mkString("\n")}")
    }
  }
}
