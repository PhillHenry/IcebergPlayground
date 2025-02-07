package uk.co.odinconsultants.iceberg
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

trait UpdatingTable {
  def tableDDL(tableName: String, mode: String, partitionField: String): String =
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
                               |    'format-version' = '2',
                               |    'write.delete.mode'='$mode',
                               |    'write.update.mode'='$mode',
                               |    'sort-order' = '$partitionField ASC NULLS FIRST',
                               |    'write.merge.mode'='$mode'
                               |) PARTITIONED BY ($partitionField); """.stripMargin

}
