package uk.co.odinconsultants.iceberg
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.insertSQL
import uk.co.odinconsultants.iceberg.TestUtils.{idField, labelField, partitionField}

class OtherTable extends TableNameFixture

class MergeIntoSpec extends SpecPretifier with GivenWhenThen with TableNameFixture with UpdatingTable {

  info("https://github.com/apache/iceberg/issues/7431")

  def mode      = "merge-on-read"

  def mergeSQL(targetTable: String, sourceTable: String): String =
    s"""MERGE INTO $targetTable t USING (SELECT * FROM $sourceTable) s
       |ON s.${idField} = t.${idField}
       |WHEN MATCHED THEN UPDATE SET t.$labelField = s.$labelField
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin

  def createTableAndPopulate(tableName: String,
                             data: Seq[Datum],
                             spark: SparkSession) = {
    val createSQL: String = tableDDL(tableName, mode, partitionField)
    val sql: String       = insertSQL(tableName, data)
    spark.sqlContext.sql(createSQL)
    spark.sqlContext.sql(sql)
  }

  s"A $mode table" should {
    s"create no new files for $mode" in new SimpleSparkFixture {
      import spark.implicits._

      val otherTable = (new OtherTable).tableName
      Given(s"tables $tableName and ${otherTable} both created as '${mode}' containing data that is the same other than the labels column")
      createTableAndPopulate(tableName, data, spark)
      val otherData = data.map(x => x.copy(label = s"${x.label}X"))
      createTableAndPopulate(otherTable, otherData, spark)

      val mergeSql = mergeSQL(tableName, otherTable)
      When(s"we merge them with:\n${formatSQL(mergeSql)}")
      val merged = spark.sqlContext.sql(mergeSql)
      private val df: Dataset[Datum] =
        spark.sqlContext.sql(s"SELECT * FROM $tableName ORDER BY $idField").as[Datum]
      Then(
        "the initial table looks like:\n" +
        captureOutputOf(df.show(truncate = false))
      )
      Then(s"the target table $tableName has the labels from the source table $otherTable")
      val dataPostMerge = df.collect()
      assert(dataPostMerge.length == data.length)
      assert(dataPostMerge.map(_.label).toSet == otherData.map(_.label).toSet)
    }
  }
}
