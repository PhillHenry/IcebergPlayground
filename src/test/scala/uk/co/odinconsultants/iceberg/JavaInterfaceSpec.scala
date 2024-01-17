package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopFileIO
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

class JavaInterfaceSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "Iceberg tables" should {
    "have its history queried via the Java APIs" in new SimpleSparkFixture {
      import spark.implicits._
      Given("a table that has seen changes")
      spark.createDataFrame(data).writeTo(tableName).create()
      val newVal    = System.currentTimeMillis().toString
      val updateSql = s"update $tableName set label='$newVal'"
      spark.sqlContext.sql(updateSql)

      When(s"we query the table")

      val table: Table = icebergTable(tableName)
      val filenames    = MetaUtils.allFilesThatmake(table, new HadoopFileIO(hadoopConfig))

      Then(
        s"the filenames are:\n${filenames.mkString("\n")}\nand they contain the most recent data"
      )

      val rows = spark.read.format("parquet").load(filenames.toArray: _*).as[Datum].collect()
      assert(rows.length == num_rows)
      rows.foreach(x => assert(x.label == newVal))
    }
  }
}
