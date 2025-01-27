package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.SpecPretifier
import uk.co.odinconsultants.iceberg.SQL.insertSQL
import uk.co.odinconsultants.iceberg.TestUtils.{idField, labelField}

class OtherTable extends TableNameFixture

class MergeIntoSpec extends SpecPretifier with GivenWhenThen with TableNameFixture with UpdatingTable {

  def mode      = "merge-on-read"

  s"A $mode table" should {
    s"create no new files for $mode" in new SimpleSparkFixture {

      private def createTableAndPopulate(tableName: String) = {
        val createSQL: String = tableDDL(tableName, mode, partitionField)
        val sql: String       = insertSQL(tableName, data)
        spark.sqlContext.sql(createSQL)
        spark.sqlContext.sql(sql)
      }

      val otherTable = new OtherTable
      Given(s"tables $tableName and ${otherTable.tableName} both created as '${mode}'")
      createTableAndPopulate(tableName)
      createTableAndPopulate(otherTable.tableName)
      s"""merge into $tableName t
        |using( select *, row_number() over(partition by $partitionField order by modified desc,dt desc)
        |as rank from spark_catalog.dw_source.s_std_trade_tidb where dt>='2023-04-20' )
        |small where rank=1) s
        |ON t.uni_order_id = s.uni_order_id and t.tenant = s.tenant and t.partner = s.partner
        |WHEN MATCHED AND s.modified>=t.modified then
        |UPDATE SET ....  WHEN NOT MATCHED THEN INSERT * ;""".stripMargin

      val mergeSql =
        s"""MERGE INTO $tableName t USING (SELECT * FROM ${otherTable.tableName}) s
           |ON s.${idField} = t.${idField}
           |WHEN MATCHED THEN UPDATE SET t.${labelField} = 'changed'
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      When(s"we merge them with:\n${formatSQL(mergeSql)}")
      val merged = spark.sqlContext.sql(mergeSql)
      Then(
        "the initial table looks like:\n" +
        captureOutputOf(spark.sqlContext.sql(s"SELECT * FROM $tableName ORDER BY $idField").show(truncate = false))
      )
    }
  }
}
