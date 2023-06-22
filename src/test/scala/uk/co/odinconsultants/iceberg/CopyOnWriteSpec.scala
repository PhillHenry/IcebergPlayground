package uk.co.odinconsultants.iceberg

class CopyOnWriteSpec extends AbstractCrudSpec {

  override def tableName = "cow_table"
  override def mode = "copy-on-write"

}
