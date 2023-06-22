package uk.co.odinconsultants.iceberg

class MergeOnReadSpec extends AbstractCrudSpec {

  override def tableName = "mor_table"
  override def mode = "merge-on-read"

}
