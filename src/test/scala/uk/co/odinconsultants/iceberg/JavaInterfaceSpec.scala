package uk.co.odinconsultants.iceberg
import org.apache.iceberg.Table
import org.apache.iceberg.data.{IcebergGenerics, Record}
import org.apache.iceberg.io.CloseableIterable
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}
import uk.co.odinconsultants.iceberg.MetaUtils.timeOrderedSnapshots

import scala.jdk.CollectionConverters._

class JavaInterfaceSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  info("see https://tabular.io/blog/java-api-part-3/")
  val colToChange = "label"
  val newVal      = System.currentTimeMillis().toString
  "Iceberg tables" should {
    "have its files visible via the Java APIs" in new SimpleSparkFixture {
      import spark.implicits._
      Given("a table that has seen changes")
      spark.createDataFrame(data).writeTo(tableName).create()
      val updateSql = s"update $tableName set $colToChange='$newVal'"
      spark.sqlContext.sql(updateSql)

      When(s"we query the table via the Java API")

      val table: Table = icebergTable(tableName)
      val filenames    = MetaUtils.allFilesThatmake(table)

      Then(
        s"the data file names are:\n${prettyPrintSampleOf(filenames)}"
      )
      And("and they contain the most recent data")
      val rows = spark.read.format("parquet").load(filenames.toArray: _*).as[Datum].collect()
      assert(rows.length == num_rows)
      rows.foreach(x => assert(x.label == newVal))
    }
    "be queried with the Java API" in new SimpleSparkFixture {
      Given(s"a table that has been changed to have column '$colToChange' set to '$newVal'")
      val table: Table = icebergTable(tableName)
      assert(valuesOfChangedColumn(IcebergGenerics.read(table).build()).toSet.head == newVal)

      val firstSnaphotId = timeOrderedSnapshots(table).head.snapshotId()
      When(s"we select an old version of the table with snapshot ID $firstSnaphotId")
      val records        = IcebergGenerics.read(table).useSnapshot(firstSnaphotId).build()
      val vals           = valuesOfChangedColumn(records)
      Then(s"no values in $colToChange are equal to '$newVal' but look like:\n${prettyPrintSampleOf(vals)}")
      assert(!vals.contains(newVal))
    }
  }
  private def valuesOfChangedColumn(
      records: CloseableIterable[Record]
  ) = {
    val vals = records.asScala.toList.map(_.getField(colToChange))
    assert(vals.length > 0)
    vals
  }
}
