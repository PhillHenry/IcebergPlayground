package uk.co.odinconsultants.iceberg

class CowAndMorSpec extends AbstractCrudSpec {

  override def tableName = "cow_table"
  override def mode = "copy-on-write"

}
